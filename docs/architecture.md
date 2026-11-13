# Architecture

## Overview

The pipeline takes four Olist e-commerce CSVs through a **Bronze → Silver → Gold**
medallion pipeline on Databricks Free Edition (PySpark + Delta Lake + Unity Catalog).
It validates the Silver orders data with **Great Expectations**, orchestrates a local mirror
of the flow with **Apache Airflow 3** (Astro CLI), and shows real-time ingestion with a local
**Apache Kafka** broker.

The four parts are deliberately loosely coupled. Each one runs on its own, and they
connect through exported files, not live integrations:

```
                       Databricks Free Edition (cloud)
 Olist CSVs ──upload──► /Volumes/workspace/bronze/raw
                          │  01_bronze_ingestion
                          ▼
                     workspace.bronze.{orders,customers,products,order_items}
                          │  02_silver_transform
                          ▼
                     workspace.silver.{orders_clean, order_items_enriched}
                          │  03_gold_aggregates                 │ 04_DataPrep_GreatExpectations
                          ▼                                     ▼
                     workspace.gold.{daily_revenue_by_category,   orders_dq.csv ──download──┐
                                     top_customers,                                         │
                                     rolling_7day_order_volume}                             │
                                                                                            ▼
 Local machine                                         great_expectations/dq_checks.py (GX 1.x)
                                                       → 5 expectations → HTML Data Docs

 Airflow (Docker, Astro)   ecommerce_pipeline: pandas mirror over airflow/include/data/*.csv
 Kafka (Docker, KRaft)     producer.py → topic new_orders → consumer.py → high_value_orders.json
```

## Components

| Component | Technology | Purpose |
|---|---|---|
| Storage and transformation | Databricks Free Edition: PySpark, Delta Lake, Unity Catalog | Bronze, Silver, and Gold tables |
| Data quality | Great Expectations 1.18.2 (pandas data source, file context) | DQ gate on Silver orders and Data Docs |
| Orchestration | Apache Airflow 3 on Astro Runtime `3.2-5` (Docker) | Schedule, retries, DQ branch |
| Streaming | Apache Kafka (`apache/kafka`, KRaft, single node) and `confluent-kafka` | Real-time order events |

## Data model

| Layer | Table | Grain | Notes |
|---|---|---|---|
| Bronze | `orders`, `customers`, `products`, `order_items` | Source rows | Schema inferred; adds `ingestion_timestamp`, `source_file` |
| Silver | `orders_clean` | 1 row per order | Deduped on `order_id`, `order_purchase_date` as DATE, null keys filtered, left-joined to customers for `customer_unique_id`, `customer_state`, `customer_city` |
| Silver | `order_items_enriched` | 1 row per item | Left join to products (null category becomes `"unknown"`), inner join to `orders_clean` |
| Gold | `daily_revenue_by_category` | day × category | Sum of `price`, excluding `canceled` and `unavailable`; Z-ordered by date |
| Gold | `top_customers` | customer (top 10) | LTV by `customer_unique_id`, plus `items_bought` |
| Gold | `rolling_7day_order_volume` | day | `order_count` and 7-row rolling average; Z-ordered by date |
| DQ input | `orders_dq.csv` | 1 row per order with items | `orders_clean` inner-joined with per-order `sum(price)` as `order_amount` |

## Design decisions

See `docs/PROJECT_CONTEXT.md` §4 for the full decision log. In short: medallion layering
for traceability, `customer_unique_id` for any per-person metric, DQ as a branch gate,
free and local infrastructure (Databricks Free Edition, local Kafka, Astro), and
overwrite-mode Delta writes for idempotent reruns.

## Infrastructure

- **Cloud:** only Databricks Free Edition (serverless compute, no cluster sizing). It uses
  catalog `workspace` with schemas `bronze`, `silver`, `gold` and volume `workspace.bronze.raw`.
- **Local:** Docker Desktop runs the Airflow containers (Postgres, scheduler, DAG processor,
  API server, triggerer) and the Kafka broker. Python venvs run GX and the Kafka clients.
- **Ports:** Airflow UI 8080, Airflow Postgres 5432, Kafka 9092 (client) and 9093 (controller,
  internal).
- **Secrets:** none are needed. Kafka is unauthenticated on localhost, the Airflow `.env`
  is empty, and Databricks is used through the browser. Anything sensitive belongs in
  gitignored `.env` files, `airflow_settings.yaml`, or `great_expectations/gx/uncommitted/`.

## Orchestration (Phase 4)

The `airflow/` directory is an Astro CLI project running Airflow locally. It defines
a single DAG, `ecommerce_pipeline` (`airflow/dags/ecommerce_pipeline.py`), with 5 tasks
plus a branch gate:

```
ingest_bronze -> transform_silver -> run_dq_checks -> build_gold -> notify_complete
                                            \-> quarantine (only if DQ checks fail)
```

- `default_args`: `retries=2`, `retry_delay=timedelta(minutes=5)`
- `schedule="@daily"`, `catchup=False`, `tags=["capstone"]`
- `run_dq_checks` is a `BranchPythonOperator` that routes to `build_gold` or
  `quarantine` based on non-null keys and referential integrity on the Silver output.
- Intermediate outputs are written to `include/output/` (`orders_clean.csv`,
  `daily_order_volume.csv`).

This DAG orchestrates a **local mirror** of the medallion pipeline. Each task runs
pandas directly against the Olist CSVs mounted at `include/data/`, standing in for
the real Bronze, Silver, and Gold Spark jobs. In production, the `PythonOperator` task
bodies would be replaced with `DatabricksRunNowOperator` calls that trigger the
actual Databricks notebooks or jobs. Airflow would then handle only scheduling, retries,
and the DQ branch gate, not the transforms themselves.

## Streaming Ingestion (Phase 5)

The `kafka/` directory runs a local single-node Kafka broker (KRaft mode, no
ZooKeeper) via Docker (`docker-compose.yml`), reachable at `localhost:9092`
with no authentication.

- `producer.py` sends 100 simulated order events to the `new_orders` topic at
  1 event per second.
- `consumer.py` (group `order-filter-1`, `auto.offset.reset=earliest`) reads `new_orders`,
  keeps events where `amount > 50`, and writes the matches to `high_value_orders.json`
  as a growing JSON micro-batch (a stand-in for a Silver landing zone).

A local broker was used instead of Confluent Cloud to avoid requiring a card
signup. The client code and Kafka concepts (topics, producer and consumer,
delivery callbacks, consumer groups, offsets) are the same as they would be
against a managed cluster. Swapping `config.py`'s `bootstrap.servers` and
adding auth is all that would change in production.
