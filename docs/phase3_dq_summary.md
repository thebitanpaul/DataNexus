# Phase 3: Data Quality Summary — Silver Orders Table

## Overview
Ran 5 Great Expectations checks against the Silver orders table (`data/orders_dq.csv`) using **Great Expectations 1.18.2**.

## Result
**All 5 expectations passed (100%).**

| # | Expectation | Column(s) | Result |
|---|---|---|---|
| 1 | Values must not be null | `order_id` | PASS |
| 2 | Values must not be null | `customer_id` | PASS |
| 3 | Values must be > 0 | `order_amount` | PASS |
| 4 | Values must fall between 2016-01-01 and 2019-01-01 | `order_purchase_date` | PASS |
| 5 | Values must not be null | `customer_unique_id` | PASS |

## Findings
- No null `order_id` or `customer_id` values — primary/foreign keys are fully populated.
- All `order_amount` values are strictly positive.
- All purchase dates fall within the expected 2016–2018 window.
- Every order maps to a known customer via a populated `customer_unique_id`, confirming referential integrity with the customer dimension.

## Tooling
- Framework: Great Expectations 1.18.2
- Script: `great_expectations/dq_checks.py`
- Data Docs report: `great_expectations/gx/uncommitted/data_docs/local_site/index.html`
