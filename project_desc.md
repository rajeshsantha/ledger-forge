# Project: LedgerForge 

## Goal
Solving production-grade data engineering problems:
- skew
- deduplication
- late data
- SCD2 joins
- performance tradeoffs

This is NOT a toy project.

---

## Tech Stack
- Spark 3.x
- Scala
- Local mode (Mac M1, 16GB)
- Parquet
- IntelliJ IDEA

---

## Existing Datasets (Synthetic, Generated via Spark)

All data lives under:
data/synthetic/

### fact_transactions
- ~50M rows
- Partitioned by event_date (30 days)
- Known issues:
    - duplicate transaction_id (~0.06%)
    - skewed account_id
    - late arrivals simulated
- Columns:
    - transaction_id
    - account_id
    - amount
    - currency
    - event_time
    - ingest_time
    - event_date

### dim_customer_scd2
- SCD Type 2
- Multiple rows per customer_id
- Columns:
    - customer_id
    - kyc_status
    - risk_tier
    - effective_from
    - effective_to
    - is_current

### dim_account
- account_id → customer_id mapping
- status may lag reality

### dim_product
- Slowly changing attributes

### dim_branch
- Highly skewed traffic

---

## Verified Facts
- 30 event_date partitions exist
- ~1,666,666 rows/day
- Parquet size ~946MB for transactions
- Data generation completed successfully

---

## Current Code Structure
- DataGenerator.scala → generates synthetic data
- Transformer trait → transformation layer
- Individual transformers per entity
- SparkSessionProvider
- JobRunner

---

## Current Learning Focus
- Batch Spark (not streaming yet)
- Understanding:
    - shuffles
    - skew
    - window functions
    - execution plans
    - correctness vs performance

---

## Open Requirements (NOT solved yet)
1. Deduplicate fact_transactions by (transaction_id, event_date)
2. Correct daily customer spend with duplicates + late data
3. Join fact_transactions to dim_customer_scd2 with temporal correctness
4. Detect high-velocity transaction patterns (batch)
5. Analyze Spark UI and explain bottlenecks

---

## How I want help
- Do NOT simplify problems
- Prefer reasoning over code unless asked
- Treat this as production-scale
- Challenge incorrect assumptions


data/
  <br> └── synthetic/
  <br> ├── branch/
  <br> ├── product/
  <br> ├── account/
  <br> ├── customer_scd2/
  <br> └── transaction/