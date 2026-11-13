# Project Context

The durable "why" behind this repo: goals, philosophy, requirements, status, and decisions.
It exists so that work can continue on any machine, or in a new Claude Code session,
without access to earlier conversations.

---

## 1. Goal

Build a **production-style, end-to-end data engineering pipeline** as a capstone and
portfolio project. It should show the core skills a data engineer is expected to have:

1. Ingest raw data faithfully (Bronze)
2. Clean, conform, and join it (Silver)
3. Produce business-ready aggregates (Gold)
4. Enforce data quality as an automated gate
5. Orchestrate the flow with retries and branching
6. Handle real-time events with a streaming broker

Dataset: [Olist Brazilian E-Commerce (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
The pipeline uses four of its CSVs: `orders`, `customers`, `products`, `order_items`.

Deliverables (from the README): Databricks notebooks with row counts and schema screenshots,
a GX suite and HTML Data Docs (5/5 passing), an Airflow DAG and a successful-run Gantt chart,
the Kafka producer/consumer and `high_value_orders.json`, this repository, and a 5-minute
walkthrough video.

## 2. Philosophy

- **Layers of increasing trust.** Bronze is never modified. Silver is cleaned. Gold is
  what the business consumes. If a number looks wrong, re-derive it downward from the
  layer you trust.
- **Correctness over convenience.** Preserve data instead of dropping it (null category
  becomes `"unknown"`). Don't fabricate data (missing delivery dates are not imputed).
  Use the semantically correct key (`customer_unique_id`).
- **Quality is a gate, not a report.** Failing checks route the run to quarantine.
- **Free, local, and reproducible.** Use free tiers and local Docker: Databricks Free
  Edition, local KRaft Kafka instead of Confluent Cloud (which needs a card), and the
  Astro CLI for Airflow. Client code and concepts match production. Only the connection
  config would change.
- **Explainability.** Code is small and commented to teach, because the repo is also a
  walkthrough and video demo. Keep that tone.
- **Idempotency.** Overwrite-mode Delta writes and a retained `dropDuplicates` mean that
  reruns are safe.

## 3. Requirements by phase, and status

| Phase | Requirement | Implementation | Status |
|---|---|---|---|
| 0 | Repo scaffold | Folder structure, `.gitignore`, `requirements.txt` | Done |
| 1 | Bronze ingestion with audit columns | `notebooks/01_bronze_ingestion.ipynb` creates schemas `bronze/silver/gold` and volume `workspace.bronze.raw`, then loads 4 CSVs to Delta with `ingestion_timestamp` and `source_file` | Done |
| 2A | Silver cleansing and joins | `notebooks/02_silver_transform.ipynb` produces `silver.orders_clean` (one row per order, 99,441 rows) and `silver.order_items_enriched` (one row per item, 112,650 rows) | Done |
| 2B | Gold aggregates (3 tables) and optimisation | `notebooks/03_gold_aggregates.ipynb` produces `gold.daily_revenue_by_category` (18,943 rows), `gold.top_customers` (top 10 by LTV), and `gold.rolling_7day_order_volume`, with `OPTIMIZE ... ZORDER BY (order_purchase_date)` on the date-grained tables | Done |
| 3 | Data quality: at least 4 checks (5 delivered) | `notebooks/04_DataPrep_GreatExpectations.ipynb` exports `orders_dq.csv` (98,666 rows, orders with items plus `order_amount`). `great_expectations/dq_checks.py` runs a GX 1.18.2 suite. 5/5 pass. | Done |
| 4 | Orchestration with retries, a schedule, and a DQ branch | `airflow/dags/ecommerce_pipeline.py`: `retries=2`, a 5-minute retry delay, `@daily`, `catchup=False`, and a `BranchPythonOperator` gate | Done |
| 5 | Streaming ingestion | `kafka/`: local KRaft broker, a producer that sends 100 events at 1 per second to `new_orders`, and a consumer that keeps orders with `amount > 50` and writes them to `high_value_orders.json` | Done |
| 6 | License and final README | Apache-2.0 `LICENSE`, full `README.md` with architecture diagram | Done |

## 4. Decision log

| Decision | Why | Trade-off |
|---|---|---|
| Medallion architecture | Failures trace to a layer, and each layer has a clear trust contract | More tables to maintain |
| Databricks **Free Edition** (serverless) | Community Edition was retired at the end of 2025. No local Spark install is needed. | Notebooks only run in Databricks |
| Group LTV by `customer_unique_id` (96,096 people), not `customer_id` (99,441 per-order IDs) | Olist mints a new `customer_id` per order. Grouping by it makes every customer look like a one-time buyer. | None. This is the only correct choice. |
| Exclude `canceled` and `unavailable` orders from revenue and LTV | No completed payment | Order-count metrics still include them |
| Null category becomes `"unknown"` (610 products) | Keeps the revenue visible and flags the gap | Adds an "unknown" bucket in reports |
| Keep `dropDuplicates` even though there are 0 duplicates | Idempotent safety net in case the source is redelivered | Negligible cost |
| Don't impute missing delivery dates (2,965 orders) | Imputing would fabricate fulfilment data | Those orders are excluded from delivery KPIs |
| Z-ORDER Gold by `order_purchase_date` | Most queries filter by date | No visible gain at this data size. Included to show the technique. |
| Run GX **locally** on an exported CSV | The HTML Data Docs open straight in a browser, and the setup is simple | The Silver data is a snapshot, not a live table |
| Airflow DAG runs a **pandas mirror** of the pipeline | Runs fully offline with no Databricks credentials | In production, task bodies would become `DatabricksRunNowOperator` calls |
| Local KRaft Kafka instead of Confluent Cloud | No card or sign-up needed. The client code is identical. | Single broker, no replication or auth |
| Remove the internal technical design document from the public repo | It contained confidential details | That document lives outside git |

## 5. Data facts worth remembering

- Raw row counts: orders 99,441 · customers 99,441 · products 32,951 · order_items 112,650.
- `customer_unique_id` has 96,096 distinct values.
- 610 products have a null `product_category_name`.
- 2,965 orders have no `order_delivered_customer_date`.
- Purchase dates fall between 2016 and 2018. The GX window is 2016-01-01 to 2019-01-01.
- `orders_dq.csv` columns: `order_id, customer_id, customer_unique_id, order_purchase_date, order_status, order_amount`.
- Order status values are lowercase, and the dataset spells it `canceled`.
