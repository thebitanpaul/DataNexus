# Known Issues, Limitations, and Next Steps

All six phases are complete and committed. This file tracks what is imperfect and what
could come next. When you fix an item, remove it here.

## Unresolved issues (verified in the code)

1. **`dq_checks.py` fails on a fresh clone and on every rerun.**
   It calls `data_sources.add_pandas("silver_orders")`, `suites.add(...)`,
   `validation_definitions.add(...)`, and `checkpoints.add(...)`. Those objects are already
   committed in `great_expectations/gx/`, so GX 1.18.2 raises
   `DataContextError: Can not write the fluent datasource silver_orders because a datasource
   of that name already exists`. (Verified by rerunning a copy of the project.)
   - Workaround, run from `great_expectations/`:
     `rm -rf gx && python dq_checks.py && git checkout -- gx`.
     The script regenerates `gx/` and passes 5/5 (verified). `git checkout` then restores the
     committed config (only UUIDs differ). The Data Docs in the ignored `gx/uncommitted/` survive.
   - Proper fix (an application-code change, not made yet): use `get`-or-`add` /
     `add_or_update` patterns for the data source, suite, validation definition, and checkpoint.
2. **Stale absolute Windows path in the `dq_checks.py` docstring** ("HOW TO RUN").
   The script itself uses relative paths (`data/orders_dq.csv`). Run it from inside
   `great_expectations/`.
3. **Notebook 03 has a stray cell.** Cell 5 evaluates `top_customers` before it is defined
   and stores a `NameError` output. "Run all" in a fresh session would fail at that cell.
   Delete the cell or move it after the Gold T2 definition.
4. **Notebooks 02 and 03 reference images** (`image_*.png`) that aren't in the repo, so they
   render as broken links on GitHub.
5. **Typo in notebook titles:** "Ctreation" in notebooks 02 and 03.
6. **`airflow/README.md` is Astro boilerplate.** It mentions an `example_astronauts` DAG that
   doesn't exist. `airflow/AirflowManual.md` holds the real short instructions.

## Limitations (by design, but worth knowing)

- The Airflow DAG doesn't call Databricks. It runs pandas on the local CSVs, and `build_gold`
  produces only `daily_order_volume.csv`, not all three Gold tables. Its DQ gate runs
  3 inline checks, not the GX suite.
- In the DAG, `notify_complete` runs only on the `build_gold` path. On `quarantine` the run
  ends without notifying, and there is no real email or Slack alert.
- Kafka is a single broker with no auth or replication. The producer generates synthetic
  orders (`cust_001..020`, `prod_001..010`), not Olist data. The consumer rewrites the whole
  JSON file on each match, and nothing loads `high_value_orders.json` into Bronze.
- GX validates a CSV snapshot exported from Databricks, not the live Delta table.
- `top_customers` isn't Z-ordered (it has no date column). Only the two date-grained Gold
  tables are.
- The Gold window function (`rolling_7day`) has no partition. Spark warns about this, and it is
  fine at this size.
- The root `requirements.txt` is an unpinned union of everything, including `pyspark` and
  `apache-airflow`, which you don't need locally. Install per component instead (see
  `docs/SETUP_MAC.md`). Known-good versions: Python 3.12, great_expectations 1.18.2,
  pandas 3.0.3, numpy 2.5.1, confluent-kafka 2.15.0. The Astro runtime image is `3.2-5`
  (Airflow 3).
- `kafka/docker-compose.yml` uses `apache/kafka:latest`, which is unpinned.

## Candidate next steps (none committed to; pick with the owner)

1. Fix issues 1, 3, and 5 above. These are small and low-risk.
2. Make `dq_checks.py` idempotent and call it (or its suite) from the Airflow
   `run_dq_checks` task, so one DQ definition serves both.
3. Swap the Airflow `PythonOperator` bodies for `DatabricksRunNowOperator` calls behind a
   Databricks connection, keeping the branch gate in Airflow.
4. Land Kafka high-value orders into Bronze, for example with Spark Structured Streaming or
   a micro-batch loader.
5. Pin versions (Kafka image tag, per-component `requirements.txt`).
6. Add the missing screenshots under `docs/` and fix the notebook image links.
7. Replace `airflow/README.md` with project-specific content.
