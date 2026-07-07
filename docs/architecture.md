# Architecture

## Overview

> TODO: Replace this section with a high-level description of the end-to-end pipeline.

## Data Flow

```
[Source Systems]
      │
      ▼
[Kafka Topics]          ← ingestion / streaming layer
      │
      ▼
[Databricks / PySpark]  ← batch & stream processing, Delta Lake
      │
      ▼
[Delta Lake / Data Warehouse]
      │
      ▼
[BI / Serving Layer]
```

## Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| Ingestion | Confluent Kafka | Real-time event streaming |
| Processing | Databricks PySpark | Batch and streaming transforms |
| Storage | Delta Lake | ACID-compliant lakehouse storage |
| Orchestration | Apache Airflow | Pipeline scheduling and monitoring |
| Data Quality | Great Expectations | Schema and value validation |

## Design Decisions

> TODO: Document key architectural choices and the trade-offs considered.

## Infrastructure

> TODO: Describe cloud provider, cluster sizing, networking, and secrets management.

## Orchestration (Phase 4)

The `airflow/` directory is an Astro CLI project running Airflow locally. It defines
a single DAG, `ecommerce_pipeline` (`airflow/dags/ecommerce_pipeline.py`), with 5 tasks
plus a branch gate:

```
ingest_bronze -> transform_silver -> run_dq_checks -> build_gold -> notify_complete
                                            \-> quarantine (only if DQ checks fail)
```

- `default_args`: `retries=2`, `retry_delay=timedelta(minutes=5)`
- `schedule="@daily"`, `catchup=False`
- `run_dq_checks` is a `BranchPythonOperator` that routes to `build_gold` or
  `quarantine` based on null/referential-integrity checks on the Silver output.

This DAG orchestrates a **local mirror** of the medallion pipeline: each task runs
pandas directly against the Olist CSVs mounted at `include/data/`, standing in for
the real Bronze/Silver/Gold Spark jobs. In production, the `PythonOperator` task
bodies would be replaced with `DatabricksRunNowOperator` calls that trigger the
actual Databricks notebooks/jobs, with Airflow only handling scheduling, retries,
and the DQ branch gate — not the transforms themselves.

## Streaming Ingestion (Phase 5)

The `kafka/` directory runs a local single-node Kafka broker (KRaft mode, no
ZooKeeper) via Docker (`docker-compose.yml`), reachable at `localhost:9092`
with no authentication.

- `producer.py` sends 100 simulated order events to the `new_orders` topic at
  1 event/sec.
- `consumer.py` reads `new_orders`, filters events where `amount > 50`, and
  writes the matches to `high_value_orders.json` as a growing JSON
  micro-batch (a stand-in for a Silver landing zone).

A local broker was used instead of Confluent Cloud to avoid requiring a card
signup. The client code and Kafka concepts (topics, producer/consumer,
delivery callbacks, consumer groups, offsets) are the same as they would be
against a managed cluster — swapping `config.py`'s `bootstrap.servers` (and
adding auth) is all that would change in production.
