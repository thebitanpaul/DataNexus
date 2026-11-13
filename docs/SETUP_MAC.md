# Setting Up on a New Machine (macOS)

The project was developed on Windows and moves to macOS without code changes. Every
component is either Databricks (cloud), Docker, or plain Python with relative paths.

## 1. Prerequisites

```bash
# Homebrew: https://brew.sh
brew install git python@3.12 astro
brew install --cask docker        # Docker Desktop. Start it once and let it finish.
```
- Python 3.11 or later. 3.12 is the version used so far. On macOS, use `python3` and `pip3`, or a venv.
- Apple Silicon is fine. `apache/kafka`, the Astro runtime image, and `confluent-kafka`
  wheels are all available for arm64.
- Databricks Free Edition account (browser only).

## 2. Clone

```bash
git clone https://github.com/thebitanpaul/DataNexus.git CapstoneProject
cd CapstoneProject
```
Claude Code picks up `CLAUDE.md` automatically. Detailed context is in `docs/`.

## 3. Restore the data files (git doesn't carry them)

`*.csv` is gitignored. Copy these from the old machine, or regenerate them:

| File | Needed by | How to get it |
|---|---|---|
| `airflow/include/data/olist_orders_dataset.csv` | Airflow DAG | Kaggle Olist dataset |
| `airflow/include/data/olist_customers_dataset.csv` | Airflow DAG | Kaggle Olist dataset |
| `airflow/include/data/olist_products_dataset.csv` | Airflow DAG | Kaggle Olist dataset |
| `airflow/include/data/olist_order_items_dataset.csv` | Airflow DAG | Kaggle Olist dataset |
| `great_expectations/data/orders_dq.csv` | `dq_checks.py` | Run notebook 04 in Databricks. It writes to `/Volumes/workspace/bronze/raw/orders_dq.csv`. Download it from there. |

```bash
mkdir -p airflow/include/data great_expectations/data
```
Leave `airflow/include/output/` alone. The DAG creates it.

Databricks tables, the volume, and the uploaded CSVs live in the Databricks workspace,
not on the PC, so nothing needs to migrate for phases 1–2.

## 4. Per-component environments

Use a separate venv per component and don't install the root `requirements.txt`. It pulls
in `pyspark` and `apache-airflow`, which you don't need locally.

```bash
# Great Expectations
cd great_expectations
python3 -m venv venv && source venv/bin/activate
pip install "great_expectations==1.18.2" pandas
# The committed gx/ makes a plain `python dq_checks.py` fail with "already exists"
# (KNOWN_ISSUES #1). Until that's fixed, regenerate gx/, then restore the committed config:
rm -rf gx && python dq_checks.py && git checkout -- gx
deactivate && cd ..

# Kafka
cd kafka
python3 -m venv .venv && source .venv/bin/activate
pip install confluent-kafka
docker compose up -d
python consumer.py       # terminal 1 (activate the venv there too)
python producer.py       # terminal 2
docker compose down
deactivate && cd ..

# Airflow (with Docker Desktop running)
cd airflow
astro dev start          # UI at http://localhost:8080
astro dev pytest         # DAG integrity tests
astro dev stop
```
Venv folders (`venv/`, `.venv/`) are gitignored. The Windows venv in
`great_expectations/venv` isn't portable, so don't copy it.

## 5. Verification checklist

- [ ] `git status` is clean after the clone (line endings are LF in the index).
- [ ] `python dq_checks.py` prints `OVERALL RESULT: PASSED`, and the Data Docs open.
- [ ] `astro dev start` comes up. Trigger `ecommerce_pipeline`, and the run is green through
      `build_gold` and `notify_complete`.
- [ ] Kafka: the consumer prints `HIGH-VALUE` lines, and `kafka/high_value_orders.json` updates.

## 6. Claude Code notes

- Claude Code's auto-memory is stored per machine and keyed by the project's absolute path,
  so it doesn't travel with the repo. `CLAUDE.md` and `docs/` are the source of truth.
- `.claude/settings.local.json` (local permission allowlist) is machine-specific and isn't
  committed. Recreate permissions on the Mac as needed.
