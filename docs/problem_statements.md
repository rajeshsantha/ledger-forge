# LedgerForge — Spark Scala Problem Statements

> 24 production-grade problems designed around the LedgerForge banking dataset.
> Each problem targets specific Spark/Scala optimization techniques (batch + streaming).

---

## Datasets Reference

| Dataset | Format | Location | Key Columns |
|---------|--------|----------|-------------|
| **customer_scd2** | Parquet | `data/synthetic/customer_scd2` | customer_id, first_name, last_name, email, phone, risk_tier, effective_start, effective_end, is_current, version |
| **account** | Parquet | `data/synthetic/account` | account_id, customer_id, branch_id, account_type, status, open_date, balance |
| **transaction** | Parquet (partitioned by `event_date`) | `data/synthetic/transaction` | transaction_id, account_id, product_id, branch_id, amount, currency, transaction_type, status, event_date, event_ts, ingest_date |
| **product** | Parquet | `data/synthetic/product` | product_id, product_name, product_group |
| **branch** | Parquet | `data/synthetic/branch` | branch_id, branch_name, state, open_date |

**Data characteristics:** ~50M transactions, ~5M customers (SCD2), ~5M accounts, 5K products, 500 branches. Intentional nulls (~2%), invalid values (~3%), duplicates (~2%), and Zipf-skewed `account_id` distribution.

---

## Section A — Joins & Lookups

### Problem 1: Branch-Level Transaction Summary

**Context:** The operations team needs a daily report showing total transaction amount and count per branch, along with the branch name and state.

**Task:**
- Join `transaction` → `account` → `branch` to get branch details for each transaction.
- Aggregate: total amount, transaction count, average amount per branch.
- Output sorted by total amount descending.

**Expected output columns:** `branch_id`, `branch_name`, `state`, `total_amount`, `txn_count`, `avg_amount`

**Constraints:**
- Handle the case where `account.branch_id` or `transaction.account_id` is null/invalid (-1).
- Filter out transactions with `status = "Reversed"`.

**Spark concepts:** Multi-table joins, filter pushdown, null handling, aggregation.

---

### Problem 2: Broadcast Join — Product Enrichment

**Context:** Every transaction must be enriched with the product name and product group. The `product` table is small (5K rows) while `transaction` is large (50M rows).

**Task:**
- Join `transaction` with `product` on `product_id`.
- First, run without any hint and capture the physical plan using `explain(true)`.
- Then, use `broadcast()` hint on the product DataFrame and capture the plan again.
- Compare: number of shuffle stages, total task count, execution time.

**Expected output columns:** All transaction columns + `product_name`, `product_group`

**Constraints:**
- Run both versions and record the difference in Spark UI (Jobs tab, Stages tab).
- Handle null/invalid `product_id` (keep the transaction, fill product columns with "Unknown").

**Spark concepts:** Broadcast join, `explain()`, physical plan analysis, Spark UI.

---

### Problem 3: SCD2 Temporal Join — Point-in-Time Customer Attributes

**Context:** For regulatory reporting, each transaction must be tagged with the customer attributes **as they were at the time of the transaction** (not the current customer record).

**Task:**
- Join `transaction` → `account` (to get `customer_id`) → `customer_scd2`.
- The join condition on `customer_scd2` must be: `event_date >= effective_start AND (event_date <= effective_end OR effective_end IS NULL)`.
- Output: enriched transactions with point-in-time customer name and risk_tier.

**Expected output columns:** `transaction_id`, `event_date`, `amount`, `customer_id`, `first_name`, `last_name`, `risk_tier`, `version`

**Constraints:**
- `effective_end` is NULL for current records — handle with `coalesce(effective_end, lit("9999-12-31"))`.
- This is a range join — Spark will not optimize it automatically. Think about how to make it efficient.
- Measure execution time with and without filtering `customer_scd2` to `is_current = true` first (incorrect but fast) vs. full SCD2 join (correct but slow).

**Spark concepts:** Range/inequality joins, SCD2 pattern, join optimization, `coalesce`.

---

## Section B — Window Functions & Deduplication

### Problem 4: Transaction Deduplication

**Context:** The data generator introduces ~2% duplicate `transaction_id` values. The same transaction may appear with different `ingest_date` values. The pipeline must keep only the **latest version** (by `ingest_date`) of each transaction.

**Task:**
- Use `row_number()` window function partitioned by `transaction_id`, ordered by `ingest_date DESC`.
- Keep only rows where `row_number = 1`.
- Count: total rows before dedup, after dedup, and number of duplicates removed.

**Expected output:** Deduplicated transaction DataFrame with same schema.

**Constraints:**
- The dedup must handle ties in `ingest_date` deterministically (add `event_ts DESC` as secondary sort).
- Compare performance of `row_number()` approach vs. `dropDuplicates("transaction_id")` — which gives correct results?

**Spark concepts:** Window functions, `row_number()`, `dropDuplicates` limitations, deterministic ordering.

---

### Problem 5: Running Account Balance

**Context:** Finance needs to see the running (cumulative) balance of each account over time, computed from transaction amounts.

**Task:**
- For each account, order transactions by `event_date` and `event_ts`.
- Compute a running sum of `amount` as `running_balance`.
- Also compute `txn_rank` (the sequence number of each transaction within the account).

**Expected output columns:** `account_id`, `transaction_id`, `event_date`, `amount`, `running_balance`, `txn_rank`

**Constraints:**
- Use `Window.partitionBy("account_id").orderBy("event_date", "event_ts")`.
- The `account_id` distribution is Zipf-skewed — some accounts will have vastly more transactions. Observe which tasks are slow in Spark UI.
- Try with `spark.sql.shuffle.partitions` set to 200, 50, and 500. Note the difference.

**Spark concepts:** Window functions (`sum`, `row_number`), `orderBy` within windows, partition skew observation.

---

### Problem 6: Fraud Velocity Detection

**Context:** The fraud team wants to flag accounts that have **5 or more transactions within any 2-minute window**.

**Task:**
- For each transaction, count how many other transactions occurred on the same account within ±2 minutes of its `event_ts`.
- Flag any transaction where this count ≥ 5 as `is_velocity_alert = true`.
- Output the flagged transactions with the velocity count.

**Expected output columns:** `transaction_id`, `account_id`, `event_ts`, `amount`, `velocity_count`, `is_velocity_alert`

**Constraints:**
- Use `Window.partitionBy("account_id").orderBy(col("event_ts").cast("long")).rangeBetween(-120, 120)` (120 seconds).
- This is computationally expensive due to skew — hot accounts will have huge windows. Measure execution time.
- Bonus: Try filtering to only "Approved" transactions before computing velocity.

**Spark concepts:** `rangeBetween` windows, timestamp-based windowing, skew impact, pre-filtering.

---

## Section C — Aggregations & Data Quality

### Problem 7: Daily Customer Spend with Late Data Handling

**Context:** The analytics team needs daily spend aggregated per customer. However, transactions can arrive up to **3 days late** (the `ingest_date` can be up to 3 days after `event_date`). The pipeline must reprocess a lookback window to correct historical aggregates.

**Task:**
- For a given run date (e.g., today), determine the reprocessing window: `[run_date - 3, run_date]`.
- Read only the `event_date` partitions in that window (partition pruning).
- Deduplicate transactions (Problem 4 logic).
- Aggregate: `customer_id`, `event_date`, `total_spend`, `txn_count`.
- Write output partitioned by `event_date`, using `overwrite` mode per partition (idempotent).

**Expected output columns:** `customer_id`, `event_date`, `total_spend`, `txn_count`

**Constraints:**
- Must use partition pruning: `df.filter(col("event_date").between(startDate, endDate))`.
- Verify partition pruning via `explain()` — look for `PushedFilters` or `PartitionFilters`.
- The write must be idempotent — running twice should produce the same result.

**Spark concepts:** Partition pruning, incremental processing, idempotent writes, `explain()` analysis.

---

### Problem 8: Data Quality Quarantine Pipeline

**Context:** Before processing, transactions must pass quality checks. Invalid records should be routed to a quarantine table with rejection reasons, while valid records continue downstream.

**Task:**
- Define quality rules:
  1. `transaction_id` is not null
  2. `account_id` is not null and > 0
  3. `amount` is not null and != -999999.99
  4. `currency` is not null and != "XXX"
  5. `event_date` is not null
  6. `transaction_type` is not null and != "UNKNOWN"
- Split the DataFrame into `valid_df` and `quarantine_df`.
- For quarantined records, add a `rejection_reasons` column (array of strings listing which rules failed).
- Use Spark **accumulators** to count how many records failed each rule.
- Print a quality summary report.

**Expected outputs:**
- `valid_df`: clean transactions, original schema
- `quarantine_df`: original schema + `rejection_reasons: Array[String]`
- Console: quality summary with counts per rule

**Constraints:**
- Do NOT filter row by row in a loop — express all rules as column expressions.
- Use `LongAccumulator` for each rule to count violations.
- Measure: what percentage of the 50M rows are quarantined?

**Spark concepts:** Column expressions, accumulators, conditional logic (`when`/`otherwise`), `array()`, `filter()`.

---

### Problem 9: UDF vs. Native — Risk Bucket Classification

**Context:** Each transaction needs a risk bucket based on `transaction_type` and `amount`:

| Condition | Risk Bucket |
|-----------|-------------|
| amount > 5000 | `CRITICAL` |
| amount > 1000 AND type = "Transfer" | `HIGH` |
| amount > 500 | `MEDIUM` |
| else | `LOW` |

**Task:**
- Implement this logic **twice**:
  1. As a Spark UDF (`udf((txnType: String, amount: Double) => ...)`)
  2. As native column expressions (`when(...).otherwise(...)`)
- Run both on the full transaction dataset. Compare:
  - Execution time
  - Physical plan (UDF prevents Catalyst optimizations)
  - Codegen (`WholeStageCodegen` present or not)

**Expected output columns:** transaction columns + `risk_bucket`

**Constraints:**
- Handle null `amount` and null `transaction_type` — both should return `UNKNOWN`.
- After comparing, explain **why native expressions are faster**.

**Spark concepts:** UDFs, Catalyst optimizer, WholeStageCodegen, `when`/`otherwise`, performance comparison.

---

## Section D — Partitioning, Bucketing & Skew

### Problem 10: Partition Optimization — Small File Problem

**Context:** After writing transactions partitioned by `event_date`, some date partitions have too many small files, and others have too few large files.

**Task:**
- Read the existing `data/synthetic/transaction` Parquet directory.
- For each `event_date` partition, count the number of files and total size.
- Identify partitions with more than 8 files (small file problem).
- Rewrite the dataset using `.repartition(col("event_date")).coalesce(4)` per partition so each date has exactly 4 files.
- Compare read performance (full scan count) before and after.

**Expected output:** Rewritten Parquet with controlled file count per partition.

**Constraints:**
- Use `spark.read.parquet(...).inputFiles` to list files.
- Note: `coalesce` after `repartition` may not work as expected — investigate why and find the correct approach.
- Try `repartition(4, col("event_date"))` instead.

**Spark concepts:** `repartition`, `coalesce`, partition file sizes, small file problem, `inputFiles`.

---

### Problem 11: Salting for Skewed Joins

**Context:** The `account_id` column in `transaction` follows a Zipf distribution — a few "hot" accounts have millions of transactions while most have very few. Joining `transaction` with `account` on `account_id` causes severe skew — a few tasks take 100x longer than others.

**Task:**
- Join `transaction` with `account` on `account_id` **without** salting. Measure time and observe skew in Spark UI (task duration variance in the join stage).
- Implement **salted join**:
  1. Add a `salt` column to `transaction`: `floor(rand() * N)` where N = 10.
  2. Explode `account` to have 10 copies, each with a different salt value (0–9).
  3. Join on `(account_id, salt)`.
  4. Drop the salt column after join.
- Compare execution times and task duration distributions.

**Expected output:** Same as a regular join, but with balanced task execution.

**Constraints:**
- Measure with `spark.time { ... }` or `RuntimeCalculator`.
- Before/after: screenshot or record max task time vs. median task time from Spark UI.
- Salting increases data volume on the dimension side by N — note the trade-off.

**Spark concepts:** Data skew, salting pattern, `explode`, Spark UI task metrics, join optimization.

---

### Problem 12: Bucketed Join

**Context:** The `transaction ↔ account` join is performed frequently. Each run reshuffles millions of rows. Pre-bucketing both tables by `account_id` can eliminate shuffle at join time.

**Task:**
- Write `account` bucketed by `account_id` into 16 buckets, sorted by `account_id`: `.bucketBy(16, "account_id").sortBy("account_id").saveAsTable("account_bucketed")`.
- Write `transaction` bucketed the same way.
- Join the two bucketed tables and inspect the physical plan — confirm there is **no Exchange (shuffle)** node.
- Compare join time: bucketed vs. non-bucketed.

**Expected output:** Joined DataFrame with no shuffle.

**Constraints:**
- Bucketed tables require `saveAsTable` (writes to Spark warehouse), not `parquet`.
- Both tables **must** use the same number of buckets and same bucket column.
- Enable `spark.sql.sources.bucketing.enabled = true` (default).
- Inspect `explain(true)` to verify `SortMergeJoin` without `Exchange`.

**Spark concepts:** Bucketing, `saveAsTable`, shuffle-free joins, physical plan inspection.

---

## Section E — Performance Tuning & Caching

### Problem 13: AQE Experiment — Adaptive Query Execution

**Context:** Spark 3.x introduced Adaptive Query Execution (AQE) which can dynamically coalesce shuffle partitions, switch join strategies, and optimize skew joins at runtime.

**Task:**
- Build a pipeline: read `transaction` → join with `account` → aggregate by `branch_id`.
- Run with `spark.sql.adaptive.enabled = false`:
  - Record: number of jobs, stages, total tasks, execution time.
  - Capture physical plan.
- Run with `spark.sql.adaptive.enabled = true`:
  - Record the same metrics.
  - Look for `AdaptiveSparkPlan`, `CustomShuffleReader`, `AQEShuffleRead` in the plan.
- Run with AQE + skew optimization: `spark.sql.adaptive.skewJoin.enabled = true`.
  - Record the same metrics. Does Spark split the skewed partitions?

**Expected output:** A comparison table of metrics for each configuration.

**Constraints:**
- Use `spark.sql.shuffle.partitions = 200` as baseline.
- With AQE on, the actual number of post-shuffle partitions will be less — record it.
- Explain what `spark.sql.adaptive.coalescePartitions.minPartitionSize` does.

**Spark concepts:** AQE, coalescing partitions, skew join optimization, physical plan analysis.

---

### Problem 14: Caching Strategy Comparison

**Context:** The deduplicated transaction DataFrame is used by 3 downstream consumers: daily aggregation, fraud detection, and data quality check. Without caching, Spark recomputes the dedup logic 3 times.

**Task:**
- Build the dedup DataFrame (Problem 4).
- Use it in 3 actions (count, aggregate, filter+count) under 4 strategies:
  1. **No caching** — let Spark recompute each time.
  2. **`.cache()`** — default `MEMORY_AND_DISK` (in Spark 3.x, `.cache()` = `MEMORY_AND_DISK`).
  3. **`.persist(StorageLevel.DISK_ONLY)`** — for memory-constrained environments.
  4. **`.checkpoint()`** — breaks the lineage (requires `spark.sparkContext.setCheckpointDir`).
- For each strategy, measure:
  - Total time for all 3 actions.
  - Storage tab in Spark UI (memory used, disk used).

**Expected output:** A comparison table of time and storage for each strategy.

**Constraints:**
- Call `.unpersist()` between experiments to reset.
- Explain when to use each strategy in production.
- Note: `.checkpoint()` is **eager** — it triggers computation immediately.

**Spark concepts:** `cache`, `persist`, `checkpoint`, `StorageLevel`, lineage, Spark UI Storage tab.

---

### Problem 15: Physical Plan Analysis — Finding Unnecessary Shuffles

**Context:** A complex pipeline joins `transaction` → `account` → `branch`, then aggregates by `state`. The execution is slow because of redundant shuffles.

**Task:**
- Build the pipeline naively:
  ```
  transaction.join(account, "account_id").join(branch, "branch_id").groupBy("state").agg(sum("amount"))
  ```
- Call `explain(true)` and identify:
  1. How many `Exchange` (shuffle) nodes are there?
  2. Which join strategy is used (SortMergeJoin, BroadcastHashJoin, etc.)?
  3. Are there unnecessary shuffles?
- Optimize:
  - Broadcast `branch` (500 rows).
  - Repartition `account` by `account_id` before join if needed.
  - Use `coalesce` before final write.
- Compare the plans before and after optimization.

**Expected output:** Optimized plan with fewer shuffles and broadcast where appropriate.

**Constraints:**
- Save the `explain(true)` output to a string for comparison.
- Count `Exchange` nodes programmatically if possible.
- Target: go from 4 shuffles to ≤ 2.

**Spark concepts:** `explain(true)`, `Exchange`, `BroadcastHashJoin`, `SortMergeJoin`, plan optimization.

---

## Section F — Incremental Processing & Production Patterns

### Problem 16: Incremental Partition Load

**Context:** In production, you don't reprocess all 50M transactions daily. You only process **new partitions** since the last successful run.

**Task:**
- Maintain a **watermark file** (`output/watermark.txt`) that stores the last processed `event_date`.
- On each run:
  1. Read the watermark (default: 30 days ago if file doesn't exist).
  2. Read only `event_date` partitions from `watermark + 1` to `today`.
  3. Process (dedup + transform).
  4. Append to the output Parquet (partitioned by `event_date`).
  5. Update the watermark file.
- The pipeline must be **idempotent** — re-running the same day should produce the same output.

**Expected output:** Incrementally growing output Parquet + updated watermark.

**Constraints:**
- Use `spark.read.parquet(...).filter(col("event_date") > lastWatermark)`.
- Write mode: `overwrite` per partition (use `partitionOverwriteMode = dynamic`).
- Handle the cold-start case (no watermark file).

**Spark concepts:** Incremental loading, partition pruning, `partitionOverwriteMode`, watermark pattern.

---

### Problem 17: Config-Driven Reusable Transformer

**Context:** Currently each entity (Customer, Account, etc.) has a hardcoded `Transformer` class. In production, transformations should be config-driven so new columns can be added without code changes.

**Task:**
- Define a HOCON config block:
  ```hocon
  transformations {
    customer {
      add_columns = [
        { name = "full_name", expr = "concat(first_name, ' ', last_name)" },
        { name = "ingest_date", expr = "current_date()" }
      ]
      drop_columns = ["version"]
      rename_columns = { "risk_tier": "customer_risk" }
    }
  }
  ```
- Implement `ConfigDrivenTransformer` that reads this config and applies:
  1. `add_columns` → `withColumn(name, expr(exprString))`
  2. `rename_columns` → `withColumnRenamed`
  3. `drop_columns` → `drop`
- The transformer should extend the existing `Transformer` trait.

**Expected output:** A transformer that works for any entity based on config.

**Constraints:**
- Use `org.apache.spark.sql.functions.expr()` to parse string expressions.
- Handle missing config keys gracefully (no crash if a section is absent).
- Write a test that validates the transformer using `scalatest`.

**Spark concepts:** `expr()`, dynamic column operations, config-driven pipelines, Typesafe Config.

---

### Problem 18: End-to-End Pipeline — Orchestrated with Metrics

**Context:** Combine all the techniques into a single end-to-end pipeline that processes transactions through multiple stages with runtime metrics.

**Task:**
Build a `PipelineOrchestrator` that executes these stages in order:
1. **Read** — Load transaction Parquet (incremental or full).
2. **Dedup** — Remove duplicate `transaction_id` (Problem 4).
3. **Quality Check** — Split valid/quarantine (Problem 8). Write quarantine to `output/quarantine/`.
4. **Enrich** — Broadcast join with `product` and `branch` (Problems 2, 15).
5. **SCD2 Join** — Temporal join with `customer_scd2` (Problem 3).
6. **Aggregate** — Daily spend per customer (Problem 7).
7. **Write** — Write enriched transactions and aggregates to `output/` partitioned by `event_date`.

For each stage, print:
- Stage name, row count in/out, duration (using `RuntimeCalculator`).
- At the end, print a pipeline summary table.

**Expected output:** Multiple output directories + console pipeline summary.

**Constraints:**
- Cache the deduplicated DataFrame (it's used by quality check and enrich stages).
- Use broadcast for small dimensions.
- Set `spark.sql.adaptive.enabled = true`.
- Target: the full pipeline should complete in under 10 minutes on a laptop (local[*] with 16GB RAM).
- Identify the bottleneck stage from your metrics.

**Spark concepts:** Pipeline orchestration, caching strategy, broadcast joins, AQE, performance measurement.

---

## Section G — Spark Structured Streaming

### Problem 19: Streaming Ingestion with Watermarking

**Context:** Transactions arrive continuously. You need to compute hourly aggregates (total amount, count) per account in real-time, handling late data that can arrive up to 3 days late.

**Task:**
- Simulate streaming by reading Parquet files with `readStream` and `maxFilesPerTrigger = 2`.
- Apply a watermark of `3 days` on `event_ts`.
- Compute a 1-hour tumbling window aggregation: `sum(amount)`, `count(*)` per `(account_id, window)`.
- Write to a Parquet sink with checkpointing.

**Expected output:** Streaming Parquet output with hourly per-account aggregates.

**Constraints:**
- Use `append` output mode (required when using watermarks with aggregations).
- Set a checkpoint directory so the stream can recover from failures.
- Test: stop the stream, restart it, and verify it resumes from where it left off.

**Spark concepts:** `readStream`, watermarks, tumbling windows, `append` mode, checkpointing, exactly-once semantics.

---

### Problem 20: Real-Time Fraud Velocity Alert (Streaming)

**Context:** The fraud team wants real-time alerts when an account has ≥5 transactions in a 2-minute window (same as Problem 6, but streaming).

**Task:**
- Stream transactions and group by `(account_id, window("event_ts", "2 minutes"))`.
- Filter groups where count ≥ 5.
- Output alerts to the console sink (or an in-memory table for querying).

**Expected output:** Console output showing account_id, window, and count for velocity alerts.

**Constraints:**
- Use `update` output mode.
- Apply a watermark of `10 minutes` to bound state store growth.
- Compare: batch version (Problem 6 with `rangeBetween`) vs. streaming version — which is simpler?

**Spark concepts:** Streaming aggregation, `update` mode, state management, watermark for state cleanup.

---

### Problem 21: Stream-Static Join — Enrich Transactions

**Context:** Enrich streaming transactions with static dimension data (product name, branch name) without reshuffling the dimension tables every micro-batch.

**Task:**
- Read `product` and `branch` as static DataFrames.
- Read `transaction` as a stream.
- Left-join the stream with the static DataFrames on `product_id` and `branch_id`.
- Write enriched transactions to Parquet.

**Expected output:** Streaming Parquet output with enriched columns.

**Constraints:**
- Static DataFrames are loaded once and broadcast automatically in stream-static joins.
- Note: you **cannot** join two streaming DataFrames with a regular join — only stream-static or stream-stream with watermarks.
- Handle null `product_id` / `branch_id` by filling with "Unknown".

**Spark concepts:** Stream-static joins, broadcast in streaming, `left` join semantics.

---

### Problem 22: Streaming Deduplication

**Context:** The transaction stream contains ~2% duplicates. Deduplicate in real-time using event-time watermarks.

**Task:**
- Use `dropDuplicatesWithinWatermark("transaction_id")` (Spark 3.5+) with a 3-day watermark on `event_ts`.
- Compare with `dropDuplicates("transaction_id")` (unbounded state — grows forever).
- Monitor state store size via Spark UI's Structured Streaming tab.

**Expected output:** Deduplicated streaming output.

**Constraints:**
- `dropDuplicatesWithinWatermark` only deduplicates within the watermark window — duplicates arriving after the watermark is evicted will pass through.
- `dropDuplicates` maintains state forever — explain why this is dangerous in production.

**Spark concepts:** `dropDuplicatesWithinWatermark`, state store, watermark-based state cleanup, memory management.

---

### Problem 23: Streaming Data Quality with `foreachBatch`

**Context:** Route each micro-batch into valid and quarantine sinks based on quality rules (same rules as Problem 8, but in streaming).

**Task:**
- Use `foreachBatch` to process each micro-batch as a regular DataFrame.
- Inside `foreachBatch`, apply the quality rules, split into valid/quarantine, and write both to separate Parquet directories.
- Use accumulators to track quality metrics across all micro-batches.

**Expected output:** Two streaming Parquet directories: `output/streaming/valid` and `output/streaming/quarantine`.

**Constraints:**
- `foreachBatch` gives you a regular DataFrame — you can use all DataFrame API features.
- Idempotency: if the same batch is retried, it should overwrite (not duplicate).
- Print a quality summary after every 10 micro-batches.

**Spark concepts:** `foreachBatch` pattern, multi-sink streaming, micro-batch idempotency, accumulators in streaming.

---

### Problem 24: Streaming Dashboard — Complete Output Mode

**Context:** Build a real-time dashboard showing running totals by `transaction_type` and `currency`, updated every micro-batch.

**Task:**
- Stream transactions and aggregate: `sum(amount)`, `count(*)` grouped by `transaction_type` and `currency`.
- Use `complete` output mode to output the entire result table each time.
- Write to `memory` sink and query with `spark.sql("SELECT * FROM dashboard ORDER BY total DESC")`.

**Expected output:** In-memory table queryable via SQL.

**Constraints:**
- `complete` mode rewrites the entire result — only feasible when the result is small (here: 4 txn types × 5 currencies = 20 rows).
- Compare output modes: `complete` vs. `update` vs. `append` — which works here and why?
- Note: `complete` mode does NOT support watermarks (all data is kept).

**Spark concepts:** `complete` output mode, `memory` sink, in-memory SQL queries, output mode constraints.

---

## Bonus Challenges

### Bonus A: Spark SQL vs. DataFrame API
Pick any 3 problems above and solve them using **Spark SQL** (register temp views, write SQL). Compare the physical plans — are they identical?

### Bonus B: Custom Partitioner
Write a custom partitioner that distributes `account_id` using consistent hashing to ensure even distribution despite Zipf skew.

### Bonus C: Delta Lake Integration
Replace Parquet reads/writes with Delta Lake. Implement `MERGE INTO` for SCD2 updates. Add time travel queries.

---

## How to Use This Document

1. Start with Section A (Joins) — these build the foundation.
2. Each problem can be implemented as a standalone Scala object with a `main` method.
3. Use `SparkSessionProvider.getSparkSession("ProblemN")` for the Spark session.
4. Check Spark UI at `http://localhost:4040` during execution.
5. Use the `RuntimeCalculator.calcRuntime` wrapper to measure each operation.
6. Solutions are in [solutions.md](solutions.md).

