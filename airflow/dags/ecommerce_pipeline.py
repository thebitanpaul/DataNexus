"""
ecommerce_pipeline.py  —  Phase 4: Airflow orchestration

Orchestrates a lightweight local mirror of the medallion pipeline:
    ingest_bronze -> transform_silver -> run_dq_checks (branch) -> build_gold -> notify_complete
                                                        \-> quarantine (only if DQ fails)

Runs inside the Astro/Airflow container. The four Olist CSVs live in
include/data/ (mounted at /usr/local/airflow/include/data).
"""

from datetime import datetime, timedelta

# Airflow 3.x import paths. If you are ever on Airflow 2.x, use:
#   from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG                     # (alt for 3.x: from airflow.sdk import DAG)
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)

# Paths inside the Airflow container
INCLUDE = "/usr/local/airflow/include"
DATA = f"{INCLUDE}/data"
OUT = f"{INCLUDE}/output"


# ---------------------------------------------------------------------------
# Task bodies (small, real pandas steps on the local CSVs)
# ---------------------------------------------------------------------------
def _ingest_bronze():
    """Bronze: read the raw CSVs untouched and log row counts."""
    import pandas as pd, os
    os.makedirs(OUT, exist_ok=True)
    for name in ["orders", "customers", "products", "order_items"]:
        df = pd.read_csv(f"{DATA}/olist_{name}_dataset.csv")
        print(f"[bronze] {name}: {len(df):,} rows")


def _transform_silver():
    """Silver: dedupe orders, standardise the date, join the real customer id."""
    import pandas as pd, os
    os.makedirs(OUT, exist_ok=True)
    orders = pd.read_csv(f"{DATA}/olist_orders_dataset.csv")
    customers = pd.read_csv(f"{DATA}/olist_customers_dataset.csv")

    orders = orders.drop_duplicates("order_id")
    orders["order_purchase_date"] = pd.to_datetime(orders["order_purchase_timestamp"]).dt.date

    silver = orders.merge(
        customers[["customer_id", "customer_unique_id", "customer_state"]],
        on="customer_id", how="left",
    )[["order_id", "customer_id", "customer_unique_id",
       "customer_state", "order_status", "order_purchase_date"]]

    silver.to_csv(f"{OUT}/orders_clean.csv", index=False)
    print(f"[silver] orders_clean: {len(silver):,} rows")


def _run_dq_checks():
    """DQ gate: run quick checks and return which task to run next."""
    import pandas as pd
    silver = pd.read_csv(f"{OUT}/orders_clean.csv")
    customers = pd.read_csv(f"{DATA}/olist_customers_dataset.csv")

    checks = {
        "no_null_order_id":       silver["order_id"].notna().all(),
        "no_null_customer_id":    silver["customer_id"].notna().all(),
        "referential_integrity":  silver["customer_id"].isin(set(customers["customer_id"])).all(),
    }
    for name, passed in checks.items():
        print(f"[dq] {name}: {'PASS' if passed else 'FAIL'}")

    # BranchPythonOperator must return the task_id to run next.
    return "build_gold" if all(checks.values()) else "quarantine"


def _build_gold():
    """Gold: one aggregate — daily order volume — written out."""
    import pandas as pd, os
    os.makedirs(OUT, exist_ok=True)
    orders = pd.read_csv(f"{OUT}/orders_clean.csv")
    volume = orders.groupby("order_purchase_date").size().reset_index(name="order_count")
    volume.to_csv(f"{OUT}/daily_order_volume.csv", index=False)
    print(f"[gold] daily_order_volume: {len(volume):,} days")


def _quarantine():
    """Only runs if DQ fails: stop before Gold."""
    print("[dq] checks FAILED — data quarantined, Gold build skipped.")


def _notify_complete():
    """Final step: log success (stand-in for an email/Slack alert)."""
    print("[notify] ecommerce_pipeline completed successfully.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "retries": 2,                          # each task retries twice on failure
    "retry_delay": timedelta(minutes=5),   # waiting 5 min between attempts
}

with DAG(
    dag_id="ecommerce_pipeline",
    description="Bronze -> Silver -> DQ gate -> Gold -> notify",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",                     # runs once a day when unpaused
    catchup=False,                         # don't backfill missed runs
    default_args=default_args,
    tags=["capstone"],
) as dag:

    ingest_bronze = PythonOperator(task_id="ingest_bronze", python_callable=_ingest_bronze)
    transform_silver = PythonOperator(task_id="transform_silver", python_callable=_transform_silver)
    run_dq_checks = BranchPythonOperator(task_id="run_dq_checks", python_callable=_run_dq_checks)
    build_gold = PythonOperator(task_id="build_gold", python_callable=_build_gold)
    quarantine = PythonOperator(task_id="quarantine", python_callable=_quarantine)
    notify_complete = PythonOperator(task_id="notify_complete", python_callable=_notify_complete)

    # Dependencies: the branch picks build_gold OR quarantine.
    ingest_bronze >> transform_silver >> run_dq_checks >> [build_gold, quarantine]
    build_gold >> notify_complete