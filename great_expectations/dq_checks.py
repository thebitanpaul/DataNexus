"""
dq_checks.py  —  Phase 3: Data Quality Gate for the Silver orders table
=======================================================================

Runs a Great Expectations (v1.x) suite against the exported Silver orders
table and produces an HTML Data Docs report.

HOW TO RUN (from inside the great_expectations folder):
    cd C:\\Users\\BitanPaul\\Documents\\DEcapstone\\great_expectations
    python dq_checks.py

INPUTS:
    data/orders_dq.csv      (exported from Databricks silver.orders_clean + order_amount)

OUTPUTS:
    gx/                     (auto-created GX project; Data Docs land inside it)
    gx/uncommitted/data_docs/local_site/index.html   (the report to screenshot)
"""

from datetime import datetime
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe


# ---------------------------------------------------------------------------
# 1. Load the exported Silver orders table, and make the date a real date.
#    (GX compares dates properly only when the column is a datetime, not text.)
# ---------------------------------------------------------------------------
df = pd.read_csv("data/orders_dq.csv")
df["order_purchase_date"] = pd.to_datetime(df["order_purchase_date"])
print(f"Loaded {len(df):,} rows from data/orders_dq.csv")


# ---------------------------------------------------------------------------
# 2. Data Context = the GX project.
#    mode="file" writes a ./gx folder so the HTML Data Docs persist on disk.
# ---------------------------------------------------------------------------
context = gx.get_context(mode="file")


# ---------------------------------------------------------------------------
# 3. Tell GX what to validate:
#    a pandas data source -> a dataframe asset -> a "whole dataframe" batch.
# ---------------------------------------------------------------------------
data_source = context.data_sources.add_pandas("silver_orders")
data_asset = data_source.add_dataframe_asset(name="orders_clean")
batch_definition = data_asset.add_batch_definition_whole_dataframe("full_table")


# ---------------------------------------------------------------------------
# 4. Expectation Suite = our data quality rules. One Expectation = one rule.
#    The brief asks for 4 checks; we write 5.
# ---------------------------------------------------------------------------
suite = context.suites.add(gx.ExpectationSuite(name="orders_silver_suite"))

# (a) keys must never be null
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id"))

# (b) every order amount must be strictly positive
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
    column="order_amount", min_value=0, strict_min=True))

# (c) purchase date must fall in the dataset's valid window
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
    column="order_purchase_date",
    min_value=datetime(2016, 1, 1), max_value=datetime(2019, 1, 1)))

# (d) referential integrity: customer_unique_id is only populated when the order
#     successfully joined to a real customer, so "not null here" proves every
#     order maps to a known customer.
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_unique_id"))


# ---------------------------------------------------------------------------
# 5. Validation Definition = "run THIS suite against THIS batch."
# ---------------------------------------------------------------------------
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="orders_validation", data=batch_definition, suite=suite))


# ---------------------------------------------------------------------------
# 6. Checkpoint = the runnable job.
#    The UpdateDataDocsAction refreshes the HTML report after each run.
# ---------------------------------------------------------------------------
checkpoint = context.checkpoints.add(gx.Checkpoint(
    name="orders_checkpoint",
    validation_definitions=[validation_definition],
    actions=[gx.checkpoint.actions.UpdateDataDocsAction(name="update_docs")],
    result_format="COMPLETE"))

result = checkpoint.run(batch_parameters={"dataframe": df})


# ---------------------------------------------------------------------------
# 7. Pass/Fail summary (console) + open the HTML report in your browser.
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"OVERALL RESULT: {'PASSED' if result.success else 'FAILED'}")
print("=" * 60)

# Per-expectation detail (wrapped defensively so a version quirk can't crash
# the run after validation already completed).
try:
    for validation_result in result.run_results.values():
        for r in validation_result["results"]:
            status = "PASS" if r["success"] else "FAIL"
            cfg = r["expectation_config"]
            col = cfg["kwargs"].get("column", "")
            print(f"  [{status}]  {cfg['type']}  ({col})")
except Exception as e:
    print("  (detailed breakdown unavailable; see Data Docs)", e)

print("\nOpening HTML Data Docs report...")
context.open_data_docs()