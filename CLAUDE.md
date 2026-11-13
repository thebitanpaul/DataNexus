# CLAUDE.md

Guidance for Claude Code when working in this repository (GitHub: `thebitanpaul/DataNexus`).

## What this is
An end-to-end **e-commerce data engineering capstone** on the public Olist Brazilian
E-Commerce dataset: Bronze → Silver → Gold medallion pipeline on Databricks, a Great
Expectations data-quality gate, an Airflow orchestration DAG, and a Kafka streaming demo.
All six planned phases are **complete**; work now is polish, fixes, and optional extensions.

Read these before non-trivial work:
- `docs/PROJECT_CONTEXT.md` — goals, philosophy, requirements, phase status, decision log
- `docs/architecture.md` — how the components actually connect (and what is simulated)
- `docs/KNOWN_ISSUES_AND_NEXT_STEPS.md` — open bugs, limitations, candidate next steps
- `docs/SETUP_MAC.md` — fresh-machine setup (macOS), data files that git does not carry
- `docs/data_quality_notes.md`, `docs/phase3_dq_summary.md` — cleansing rules and DQ results

## Layout
| Path | What | Runs where |
|---|---|---|
| `notebooks/01–03_*.ipynb` | Bronze / Silver / Gold in PySpark + Delta | Databricks Free Edition only (uses `spark`, `%sql`, `/Volumes/...`) |
| `notebooks/04_DataPrep_GreatExpectations.ipynb` | Exports `orders_dq.csv` for GX | Databricks |
| `great_expectations/dq_checks.py` | GX 1.x suite (5 checks) + Data Docs | Local Python, reads `great_expectations/data/orders_dq.csv` |
| `airflow/` | Astro CLI project; DAG `dags/ecommerce_pipeline.py` | Docker via `astro dev start` (UI on :8080) |
| `kafka/` | `docker-compose.yml` (KRaft broker on :9092), `producer.py`, `consumer.py`, `config.py` | Docker + local Python |
| `scripts/` | `fetch_data.sh` (gets the gitignored data), `build_orders_dq.py` | Local bash + Python |

## Required data files (gitignored, so fetch them first on a fresh clone)
The project won't run until these exist. **On a fresh clone, run this before anything else:**
```bash
bash scripts/fetch_data.sh        # needs curl, unzip, and python3 with pandas (or PYTHON=<venv python>)
```
| File | Location | Source |
|---|---|---|
| `olist_orders_dataset.csv` | `airflow/include/data/` | Kaggle `olistbr/brazilian-ecommerce` (public URL, no login) |
| `olist_customers_dataset.csv` | `airflow/include/data/` | same zip |
| `olist_products_dataset.csv` | `airflow/include/data/` | same zip |
| `olist_order_items_dataset.csv` | `airflow/include/data/` | same zip |
| `orders_dq.csv` | `great_expectations/data/` | Built locally by `scripts/build_orders_dq.py` (an exact pandas mirror of notebook 04) |

The script is idempotent: it skips the download when the SHA-256 checksums already match.
Pass `--force` to download again. Manual fallback:
`curl -L -o olist.zip https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce`, then
`unzip -j olist.zip <the 4 files> -d airflow/include/data`. For Databricks (phases 1–2), upload the
same 4 CSVs to the volume `workspace.bronze.raw`.

## Commands
```bash
# Data quality (from great_expectations/)
python3 -m venv venv && source venv/bin/activate
pip install "great_expectations==1.18.2" pandas
rm -rf gx && python dq_checks.py && git checkout -- gx   # plain rerun fails: committed gx/ (known issue #1)

# Airflow (from airflow/) — needs Docker running and the Olist CSVs in include/data/
astro dev start      # http://localhost:8080
astro dev pytest     # runs airflow/tests (DAG import, tags, retries>=2)
astro dev stop       # or: astro dev kill (remove containers)

# Kafka (from kafka/)
docker compose up -d
pip install confluent-kafka
python consumer.py   # terminal 1
python producer.py   # terminal 2
docker compose down
```
There is no unified test suite, linter, or build step. Notebooks cannot run locally.

## Invariants (do not break)
- Lifetime value / customer metrics group by **`customer_unique_id`**, never `customer_id`
  (Olist issues a new `customer_id` per order).
- Revenue and LTV exclude `order_status` in (`"canceled"`, `"unavailable"`). The dataset
  spells it **`canceled`** (one *l*); match that in code.
- Null product categories become `"unknown"` and are never dropped, so revenue is kept.
- Bronze is an untouched copy plus audit columns `ingestion_timestamp`, `source_file`.
- Tables use Unity Catalog three-part names: `workspace.<bronze|silver|gold>.<table>`.
  Writes are Delta with `mode("overwrite")`, so reruns are idempotent full refreshes.
- DQ is a **gate**: the DAG's `run_dq_checks` is a `BranchPythonOperator` returning
  `"build_gold"` or `"quarantine"`. Bad data must not reach Gold.
- Every DAG needs `tags` and `default_args["retries"] >= 2` (enforced by `airflow/tests`).

## Conventions
- Small, readable, heavily commented scripts. The commenting style is explanatory, almost
  tutorial-like, because the repo doubles as a portfolio and walkthrough piece. Keep it.
- Each script starts with a module docstring: purpose, phase number, how to run, inputs and outputs.
- Airflow: Airflow 3 import paths (`airflow.providers.standard.operators.python`); imports
  such as pandas go **inside** task callables to keep DAG parsing fast; container paths
  live under `/usr/local/airflow/include`.
- PySpark: alias builtin-shadowing functions (`sum as _sum`, `round as _round`); every code
  cell sits under a markdown cell with short bullets that explain *why*.
- Kafka connection settings live only in `kafka/config.py`.
- Commit messages are imperative and phase-tagged, e.g. `Add Kafka streaming ingestion (Phase 5): ...`.

## Guardrails
- **Never commit data or secrets.** `.gitignore` excludes `*.csv`, `*.parquet`, `.env`,
  `*.key`, venvs, `great_expectations/gx/uncommitted/`, Airflow logs and the DB. The Olist CSVs
  and `orders_dq.csv` come from `scripts/fetch_data.sh` (see "Required data files" above).
- The internal technical design document was deliberately removed from this public repo
  (it held confidential details). Don't re-add it or reproduce its contents.
- Keep documentation free of machine-specific absolute paths. Use repo-relative paths.
- README's "About phiUture" section and LICENSE (Apache-2.0) are owner-maintained branding.
  Leave them alone unless asked.
