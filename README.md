# Data Engineering Capstone Project

## Architecture

> TODO: Describe the overall system architecture — data sources, ingestion layer, processing, storage, and serving.

## Setup

### Prerequisites

- Python 3.10+
- Apache Airflow
- Databricks workspace (or local Spark)
- Kafka cluster (or Confluent Cloud)
- Great Expectations

### Installation

```bash
pip install -r requirements.txt
```

> TODO: Add environment-specific setup steps (env vars, secrets, connections).

## How to Run Each Phase

### Phase 1: Ingestion (Kafka)

> TODO: Document how to start producers/consumers and configure topics.

```bash
# Example
cd kafka/
python producer.py
```

### Phase 2: Processing (Databricks / PySpark)

> TODO: Document how to attach notebooks to a cluster and run them in order.

1. Open `notebooks/` in your Databricks workspace.
2. Run notebooks in sequence: `01_ingest → 02_transform → 03_load`.

### Phase 3: Orchestration (Airflow)

> TODO: Document DAG deployment and trigger steps.

```bash
# Copy DAGs to Airflow home
cp airflow/dags/*.py $AIRFLOW_HOME/dags/
airflow dags trigger capstone_pipeline
```

### Phase 4: Data Quality (Great Expectations)

> TODO: Document how to run checkpoint validations.

```bash
cd great_expectations/
great_expectations checkpoint run capstone_checkpoint
```

## Project Structure

```
CapstoneProject/
├── notebooks/          # Databricks notebooks (PySpark)
├── airflow/            # DAGs and Airflow configs
├── kafka/              # Producers, consumers, topic configs
├── great_expectations/ # GE project, suites, checkpoints
├── docs/               # Architecture diagrams and design docs
├── requirements.txt
└── README.md
```
