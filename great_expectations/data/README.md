# great_expectations/data

`dq_checks.py` reads `orders_dq.csv` from here (98,666 rows). Its columns are
`order_id, customer_id, customer_unique_id, order_purchase_date, order_status, order_amount`.

The file is **gitignored** (`*.csv`). You can get it either way:

- **Locally (recommended):** run `bash scripts/fetch_data.sh` from the repo root. It downloads
  the Olist CSVs and rebuilds this file with `scripts/build_orders_dq.py`, a pandas mirror
  of notebook 04 that has been verified to match the Databricks export exactly.
- **From Databricks:** run `notebooks/04_DataPrep_GreatExpectations.ipynb`, then download
  `/Volumes/workspace/bronze/raw/orders_dq.csv` into this folder.
