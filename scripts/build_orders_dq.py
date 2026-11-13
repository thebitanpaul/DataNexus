"""
build_orders_dq.py — rebuild great_expectations/data/orders_dq.csv locally.

A pandas mirror of notebooks/04_DataPrep_GreatExpectations.ipynb (plus the
orders_clean logic from notebook 02), so the Great Expectations step can run
without Databricks. Verified to reproduce the Databricks export exactly
(98,666 rows; identical keys, dates, statuses and amounts).

Run:  python scripts/build_orders_dq.py <olist_csv_dir> <output_csv>
      (normally called by scripts/fetch_data.sh)
"""

import sys

import pandas as pd

src, out = sys.argv[1], sys.argv[2]

orders = pd.read_csv(f"{src}/olist_orders_dataset.csv")
customers = pd.read_csv(f"{src}/olist_customers_dataset.csv")
items = pd.read_csv(f"{src}/olist_order_items_dataset.csv")

# Silver orders_clean: dedupe, keys must exist, standardise date, attach the real customer
orders = orders.drop_duplicates("order_id").dropna(subset=["order_id", "customer_id"])
orders["order_purchase_date"] = pd.to_datetime(orders["order_purchase_timestamp"]).dt.date
orders = orders.merge(customers[["customer_id", "customer_unique_id"]],
                      on="customer_id", how="left")

# order_amount = each order's total item price; inner join keeps orders that have items
amount = (items.groupby("order_id", as_index=False)["price"].sum()
          .rename(columns={"price": "order_amount"}))
amount["order_amount"] = amount["order_amount"].round(2)

orders_dq = orders.merge(amount, on="order_id", how="inner")[
    ["order_id", "customer_id", "customer_unique_id",
     "order_purchase_date", "order_status", "order_amount"]]

orders_dq.to_csv(out, index=False)
print(f"[build] wrote {len(orders_dq):,} rows -> {out}")
