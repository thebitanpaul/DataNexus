# Data Quality Notes — Olist Silver / Gold Pipeline

## Silver Layer Cleansing Decisions

### Product Category — Null Relabeling
- **Finding:** 610 products had a null `product_category_name`.
- **Decision:** Relabeled to `"unknown"` rather than dropping the rows.
- **Rationale:** These products have associated orders and real revenue. Dropping them would silently under-count GMV. The `"unknown"` label keeps them visible in category-level reports so analysts know the gap exists.

### Order Deduplication
- **Finding:** 0 duplicate `order_id` rows detected.
- **Decision:** `dropDuplicates()` call retained in the pipeline.
- **Rationale:** The step is idempotent — it costs nothing when no duplicates exist and protects against double-counting if the source dataset is ever re-delivered or re-ingested with overlapping records.

### Orders Without a Delivery Date
- **Finding:** 2,965 orders have no `order_delivered_customer_date`.
- **Decision:** Rows kept as-is; no imputation applied.
- **Rationale:** Missing delivery dates are expected — they correspond to orders that are undelivered, in transit, or cancelled. Imputing a date would fabricate fulfillment data. These orders are still valid for order-count metrics; they are excluded only from delivery-time KPIs (see Gold exclusions below).

---

## Gold Layer Exclusion Decisions

### Revenue and Lifetime Value — Cancelled / Unavailable Orders
- **Decision:** Orders with `order_status` of `cancelled` or `unavailable` are excluded from all revenue aggregations and customer lifetime value (LTV) calculations.
- **Rationale:** Cancelled and unavailable orders do not result in a completed payment. Including them would inflate GMV and LTV figures, misrepresenting actual business performance.

### Lifetime Value — Customer Granularity
- **Decision:** LTV is grouped by `customer_unique_id` (96,096 distinct people), not `customer_id` (99,441 per-order surrogate IDs).
- **Rationale:** Olist assigns a new `customer_id` for each order, so aggregating by `customer_id` yields a per-order "lifetime" value of exactly one order for every customer — analytically meaningless. `customer_unique_id` correctly identifies the same individual across multiple orders and produces a true lifetime revenue figure.

---

## Gold Table Optimisation

### Z-ORDER by `order_purchase_date`
- All Gold Delta tables are Z-ORDERed on `order_purchase_date`.
- **Rationale:** Most analytical queries filter or sort by purchase date (time-series trend, cohort, period-over-period). Z-ORDER co-locates data by this column in Delta's file layout, reducing files scanned and improving query latency.
