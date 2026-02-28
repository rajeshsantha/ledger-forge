# LedgerForge — Data Model & Insights Guide

> A plain-English explanation of the datasets, their relationships, data quirks,
> and what questions you can answer with them — including Spark Structured Streaming ideas.

---

## The Business Story

Imagine you're building the data platform for **LedgerForge Bank**. The bank has:

- **Customers** who open **accounts** at **branches**.
- Each account is linked to **products** (Checking, Savings, Credit Card, etc.).
- Customers perform **transactions** (deposits, withdrawals, transfers, payments) on their accounts.

Your job is to build pipelines that read this raw data, clean it, enrich it, and produce analytics.

---

## The 5 Datasets — Explained Simply

### 1. `branch` — Where the bank operates

```
branch_id | branch_name       | state | open_date
----------|-------------------|-------|----------
1         | Branch Downtown   | NY    | 2018-05-12
2         | Branch Airport    | CA    | 2020-11-03
```

- **500 rows** — the smallest dataset. Think of it as a lookup/dimension table.
- Each branch has a unique `branch_id`, a name, a state, and the date it was opened.
- **Data quirks:** ~2% of `state` values are NULL, ~3% are "ZZ" (invalid).

**Real-world analogy:** Like a list of all Starbucks store locations.

---

### 2. `product` — What the bank sells

```
product_id | product_name  | product_group
-----------|---------------|-------------
1          | Checking      | Deposit
2          | Auto Loan     | Loan
3          | Credit Card   | Deposit
```

- **5,000 rows** — another small dimension table.
- `product_group` is either "Loan" or "Deposit" based on the product name.
- **Data quirks:** ~2% of `product_name` are NULL, ~3% are "UNKNOWN".

**Real-world analogy:** Like a product catalog (iPhone, MacBook, iPad → grouped as "Electronics").

---

### 3. `customer_scd2` — Who the bank serves (with history!)

```
customer_id | first_name | last_name | email                  | phone        | risk_tier | effective_start | effective_end | is_current | version
------------|------------|-----------|------------------------|--------------|-----------|-----------------|---------------|------------|--------
42          | Alice      | Smith     | alice.smith42@ex.com   | 555-123-4567 | Low       | 2025-03-15      | 2026-02-09    | false      | 1
42          | Alice      | Smith     | alice.smith42@ex.com   | 555-123-4567 | High      | 2026-02-10      | NULL          | true       | 2
```

- **~5.75M rows** (5M customers, but ~15% have 2 versions = ~5.75M total rows).
- This is an **SCD Type 2** (Slowly Changing Dimension) table. It tracks history:
  - Customer 42 was `Low` risk from March 2025 to Feb 9, 2026 (version 1, `is_current = false`).
  - Customer 42 became `High` risk on Feb 10, 2026 (version 2, `is_current = true`).
  - `effective_end = NULL` means "this is the current active record."

**Why SCD2 matters:** If a transaction happened on Jan 15, 2026, you need to know the customer's risk tier _on that date_, not today. This requires a temporal/range join.

**Data quirks:** ~2% of `email` are NULL or "invalid_email". ~2% of `phone` are NULL or "000-000-0000".

**Real-world analogy:** Like how your credit score changes over time — FICO keeps every version with dates.

---

### 4. `account` — The link between customers and branches

```
account_id | customer_id | branch_id | account_type | status  | open_date  | balance
-----------|-------------|-----------|--------------|---------|------------|--------
1          | 4532        | 42        | Checking     | Active  | 2022-01-15 | 5420.50
2          | 4532        | 42        | Savings      | Active  | 2022-01-15 | 12000.00
3          | 891         | 7         | Loan         | Closed  | 2019-06-20 | -3500.00
```

- **5M rows** — medium-sized table. One customer can have multiple accounts.
- `status` is one of: Active, Dormant, Closed.
- `account_type` is one of: Checking, Savings, Loan, Credit.
- `balance` ranges from -$2,000 to $18,000 (negative = owed).

**This table is the central hub.** It connects:
- `customer_id` → links to `customer_scd2`
- `branch_id` → links to `branch`
- `account_id` → links to `transaction`

**Data quirks:** ~2% of `customer_id` are NULL or -1 (invalid). ~3% of `account_type` are "INVALID_TYPE".

**Real-world analogy:** Like your bank accounts — you might have a checking account, savings account, and a mortgage, all at the same bank.

---

### 5. `transaction` — What happens every day (the BIG table)

```
transaction_id | account_id | product_id | branch_id | amount  | currency | transaction_type | status   | event_date | event_ts             | ingest_date
---------------|------------|------------|-----------|---------|----------|------------------|----------|------------|----------------------|------------
1              | 42         | 3          | 7         | 250.00  | USD      | Deposit          | Approved | 2026-02-15 | 2026-02-15 14:32:05  | 2026-02-15
2              | 42         | 3          | 7         | -50.00  | USD      | Withdrawal       | Approved | 2026-02-15 | 2026-02-15 16:45:22  | 2026-02-17
```

- **50M rows** — the largest dataset. This is the **fact table**.
- Partitioned by `event_date` (30 date partitions), so Spark can read just the dates you need.
- `event_ts` is the exact timestamp when the transaction happened.
- `ingest_date` is when the data was loaded into the system (can be up to 3 days after `event_date` — this simulates late-arriving data).

**Data quirks (this is where it gets interesting):**
- **~2% duplicates:** The same `transaction_id` appears multiple times with different `ingest_date` values. You must deduplicate.
- **~2% NULL values** in `account_id`, `currency`, `amount`, `transaction_type`.
- **~3% invalid values:** `account_id = -1`, `currency = "XXX"`, `amount = -999999.99`, `transaction_type = "UNKNOWN"`.
- **Zipf-skewed `account_id`:** A few "hot" accounts have thousands of times more transactions than average accounts. This causes data skew in joins and aggregations.

**Real-world analogy:** Like your bank statement — every swipe, transfer, and ATM withdrawal.

---

## How the Tables Connect (Entity Relationships)

```
                    ┌──────────┐
                    │  branch  │
                    │ (500)    │
                    └────┬─────┘
                         │ branch_id
                         │
┌──────────────┐    ┌────┴─────┐    ┌─────────────────┐
│ customer_scd2│    │ account  │    │   product        │
│ (5.75M)      │◄───┤ (5M)     │    │   (5K)           │
│              │    │          │    │                   │
└──────────────┘    └────┬─────┘    └────┬──────────────┘
  customer_id ▲          │ account_id     │ product_id
              │          │               │
              │     ┌────┴───────────────┴──┐
              │     │     transaction        │
              └─────┤     (50M)              │
                    │  (via account.cust_id) │
                    └────────────────────────┘
```

**Join paths:**
1. **transaction → account** (on `account_id`) — Get which customer and branch the transaction belongs to.
2. **account → customer_scd2** (on `customer_id`) — Get customer details. Needs date range for SCD2.
3. **account → branch** (on `branch_id`) — Get branch location.
4. **transaction → product** (on `product_id`) — Get product name/group.
5. **transaction → account → branch** — Full chain to get branch info for a transaction.
6. **transaction → account → customer_scd2** — Full chain to get customer info at transaction time.

---

## What Insights Can You Generate?

### Business Analytics (Aggregation & Joins)

| Question | Tables Needed | Spark Technique |
|----------|---------------|-----------------|
| Total revenue by branch and state? | transaction + account + branch | Multi-join + groupBy |
| Which products generate the most revenue? | transaction + product | Join + agg |
| Top 10 customers by total spending? | transaction + account + customer | Join + groupBy + orderBy |
| Monthly transaction trend (volume & amount)? | transaction | groupBy(month) + agg |
| Which branches have the most dormant accounts? | account + branch | filter + groupBy |
| Average balance by account type? | account | groupBy + avg |
| Which states have the highest transaction volume? | transaction + account + branch | 3-table join + groupBy |
| Revenue split by currency? | transaction | groupBy("currency") |
| Customer churn — accounts that went from Active to Closed? | account + customer | filter + join |
| How many customers changed risk tier (SCD2 version > 1)? | customer_scd2 | filter(version > 1) |

### Data Quality & Operational Insights

| Question | Tables Needed | Spark Technique |
|----------|---------------|-----------------|
| What % of transactions have invalid/null data? | transaction | filter + count |
| How many duplicate transaction_ids exist? | transaction | groupBy + having(count > 1) |
| Which accounts have the most fraudulent-looking velocity? | transaction | Window + rangeBetween |
| How late does data arrive (gap between event_date and ingest_date)? | transaction | datediff + histogram |
| Which branches have the most data quality issues? | transaction + account + branch | Join + quality rules |

### Time-Series & Trend Analysis

| Question | Tables Needed | Spark Technique |
|----------|---------------|-----------------|
| Daily running balance per account? | transaction | Window + cumulative sum |
| 7-day rolling average of transaction amount per account? | transaction | Window + rangeBetween |
| Peak transaction hours of the day? | transaction | hour(event_ts) + groupBy |
| Weekend vs. weekday spending patterns? | transaction | dayofweek + groupBy |
| Is there a correlation between account age and transaction volume? | account + transaction | join + datediff + corr |

---

## Can This Data Be Used for Spark Structured Streaming?

**Yes, absolutely.** The data is designed to simulate streaming naturally. Here's how:

### Why the Data Fits Streaming

1. **`transaction` is partitioned by `event_date`** — Each date partition can simulate one "micro-batch" or a streaming window.
2. **`event_ts` is a precise timestamp** — This is your **event time** for windowed aggregations.
3. **`ingest_date` can lag `event_date` by 0-3 days** — This naturally models **late-arriving data** and **watermarking**.
4. **Duplicates exist** — Streaming pipelines need deduplication too.

### Streaming Problem Ideas

#### Streaming Problem 1: Real-Time Transaction Ingest with Watermarking

Simulate a streaming source by reading Parquet partitions as a `rate` source or using `readStream` on a directory. Apply a watermark of 3 days on `event_ts` and compute windowed aggregations:

```scala
val stream = spark.readStream
  .schema(transactionSchema)
  .parquet("data/synthetic/transaction")

val windowed = stream
  .withWatermark("event_ts", "3 days")
  .groupBy(window(col("event_ts"), "1 hour"), col("account_id"))
  .agg(sum("amount").as("hourly_total"), count("*").as("txn_count"))

windowed.writeStream
  .outputMode("append")
  .format("parquet")
  .option("path", "output/streaming/hourly_agg")
  .option("checkpointLocation", "output/streaming/checkpoint")
  .start()
```

**What you learn:** Watermarks, event-time windows, append output mode, checkpointing.

---

#### Streaming Problem 2: Real-Time Fraud Velocity Alert

Stream transactions and flag accounts with ≥5 transactions in a 2-minute tumbling window:

```scala
val alerts = stream
  .withWatermark("event_ts", "10 minutes")
  .groupBy(window(col("event_ts"), "2 minutes"), col("account_id"))
  .count()
  .filter(col("count") >= 5)

alerts.writeStream
  .outputMode("update")
  .format("console")
  .start()
```

**What you learn:** Tumbling windows vs. sliding windows, update output mode, real-time alerting.

---

#### Streaming Problem 3: Stream-Static Join — Enrich with Dimensions

Join the streaming transaction data with static dimension tables (product, branch):

```scala
val products = spark.read.parquet("data/synthetic/product") // static
val enriched = stream.join(products, Seq("product_id"), "left")

enriched.writeStream
  .outputMode("append")
  .format("parquet")
  .option("path", "output/streaming/enriched")
  .option("checkpointLocation", "output/streaming/enrich_checkpoint")
  .start()
```

**What you learn:** Stream-static joins (streaming side LEFT JOIN static side), no shuffle needed for the static side.

---

#### Streaming Problem 4: Streaming Deduplication

Deduplicate the stream on `transaction_id` using `dropDuplicates` with a watermark:

```scala
val deduped = stream
  .withWatermark("event_ts", "3 days")
  .dropDuplicatesWithinWatermark("transaction_id")
```

**What you learn:** `dropDuplicatesWithinWatermark` (Spark 3.5+), state store management, watermark-based cleanup.

---

#### Streaming Problem 5: Streaming Data Quality Monitor

Split streaming transactions into valid and quarantine sinks using `foreachBatch`:

```scala
stream.writeStream
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    val valid = batchDF.filter(col("amount").isNotNull && col("amount") =!= -999999.99)
    val quarantine = batchDF.filter(col("amount").isNull || col("amount") === -999999.99)

    valid.write.mode("append").parquet("output/streaming/valid")
    quarantine.write.mode("append").parquet("output/streaming/quarantine")
  }
  .option("checkpointLocation", "output/streaming/dq_checkpoint")
  .start()
```

**What you learn:** `foreachBatch` pattern, multi-sink streaming, micro-batch processing.

---

#### Streaming Problem 6: Real-Time Dashboard Metrics (Complete Mode)

Maintain a running aggregate of total amount by `transaction_type` and `currency`:

```scala
val metrics = stream
  .groupBy("transaction_type", "currency")
  .agg(sum("amount").as("total"), count("*").as("volume"))

metrics.writeStream
  .outputMode("complete")
  .format("console")  // or "memory" for querying via spark.sql
  .start()
```

**What you learn:** Complete output mode (rewrites entire result each micro-batch), in-memory sink for interactive queries.

---

### How to Simulate Streaming from Parquet Files

Since the data is already on disk, you can simulate streaming by:

**Option A: File-based streaming** — Drop Parquet files into a directory and read with `readStream`:
```scala
val stream = spark.readStream
  .schema(transactionSchema)
  .option("maxFilesPerTrigger", 2) // process 2 files per micro-batch
  .parquet("data/streaming_input/")
```

**Option B: Rate source** — Generate synthetic events at a controlled rate:
```scala
val rate = spark.readStream
  .format("rate")
  .option("rowsPerSecond", 1000)
  .load()
```

**Option C: Copy partitions incrementally** — Write a script that copies one `event_date` partition at a time into a watched directory, simulating daily data arrival.

---

## Summary: How to Think About This Data

| Concept | In LedgerForge Terms |
|---------|---------------------|
| **Fact table** | `transaction` (50M rows, what happened) |
| **Dimension tables** | `customer_scd2`, `account`, `product`, `branch` (who, where, what) |
| **Star schema center** | `account` is the hub connecting customer ↔ branch ↔ transaction |
| **SCD2** | `customer_scd2` tracks attribute changes over time |
| **Event time** | `event_ts` — when the transaction actually occurred |
| **Processing time** | `ingest_date` — when the system received the record |
| **Late data** | `ingest_date - event_date` can be 0-3 days |
| **Dirty data** | NULLs, -1 IDs, "XXX" currency, "UNKNOWN" types, -999999.99 amounts |
| **Skewed data** | Zipf distribution on `account_id` — a few accounts dominate |
| **Duplicates** | ~2% of transaction_ids appear multiple times |

With this understanding, you should be able to come up with your own problems. Just pick any business question from the tables above, think about which tables you need to join, what data quality issues you'll hit, and what Spark technique handles it best.

