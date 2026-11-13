# airflow/include/data

The Airflow DAG (`dags/ecommerce_pipeline.py`) reads these four CSVs from here
(mounted in the container at `/usr/local/airflow/include/data`):

| File | Rows (excl. header) |
|---|---|
| `olist_orders_dataset.csv` | 99,441 |
| `olist_customers_dataset.csv` | 99,441 |
| `olist_products_dataset.csv` | 32,951 |
| `olist_order_items_dataset.csv` | 112,650 |

They are **gitignored** (`*.csv`). To download and verify them, run from the repo root:

```bash
bash scripts/fetch_data.sh
```

Source: Kaggle [olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
The script uses the public download URL, so it doesn't need a Kaggle login. The zip holds
9 CSVs, and only these 4 are used.
