# LedgerForge — Spark Optimization Experiments Guide

> A hands-on lab for learning Spark internals through 8 controlled experiments.
> Each experiment runs a **baseline** (naive) and **optimized** implementation,
> prints physical plans, timing, and a comparison summary.

---

## Prerequisites

1. **Generate synthetic data** (if not already present):
   ```bash
   # From the project root — generates ~50M transactions, 5M customers, etc.
   mvn package -DskipTests
   spark-submit --class com.ledgerforge.pipeline.utils.DataGenerator \
     target/ledger-forge-1.0-SNAPSHOT.jar
   ```
   Or with smaller data for quick iteration:
   ```bash
   spark-submit --class com.ledgerforge.pipeline.utils.DataGenerator \
     target/ledger-forge-1.0-SNAPSHOT.jar \
     numTransactions=1000000 numCustomers=100000 numAccounts=100000
   ```

2. **Spark UI** — Experiments enable the Spark UI at `http://localhost:4040`.
   Keep this open in a browser while running experiments to observe stages,
   tasks, shuffle sizes, and cached DataFrames.

3. **Build** — All experiments compile with the existing `pom.xml` (no new dependencies).

---

## How to Run an Experiment

### From IntelliJ IDEA
Right-click any `ExperimentXX_*.scala` object → Run.

### From Terminal (spark-submit)
```bash
mvn package -DskipTests

# Example: run Experiment 01
spark-submit --class com.ledgerforge.experiments.Experiment01_SkewedJoins \
  --master "local[*]" \
  target/ledger-forge-1.0-SNAPSHOT.jar
```

### From Terminal (direct java — for local[*] mode)
```bash
mvn package -DskipTests

java -cp target/ledger-forge-1.0-SNAPSHOT.jar \
  com.ledgerforge.experiments.Experiment01_SkewedJoins
```

---

## Experiment Index

| # | Name | Spark Concept | Baseline | Optimized |
|---|------|---------------|----------|-----------|
| 01 | [Skewed Joins](../src/main/scala/com/ledgerforge/experiments/Experiment01_SkewedJoins.scala) | Data skew, salting | Naive equi-join on skewed `account_id` | Salted join (N=10) |
| 02 | [Broadcast Joins](../src/main/scala/com/ledgerforge/experiments/Experiment02_BroadcastJoins.scala) | Broadcast, join strategies | SortMergeJoin (auto-broadcast disabled) | BroadcastHashJoin |
| 03 | [Partition Pruning](../src/main/scala/com/ledgerforge/experiments/Experiment03_PartitionPruning.scala) | Partition filters, predicate pushdown | Full scan (all 30 partitions) | Filter on partition column |
| 04 | [Caching Strategies](../src/main/scala/com/ledgerforge/experiments/Experiment04_CachingStrategies.scala) | cache, persist, checkpoint | No caching (recompute 3×) | cache / DISK_ONLY / checkpoint |
| 05 | [Adaptive Query Execution](../src/main/scala/com/ledgerforge/experiments/Experiment05_AQE.scala) | AQE, coalescing, skew join | AQE off (static plan) | AQE on / AQE + skew |
| 06 | [Deduplication Strategies](../src/main/scala/com/ledgerforge/experiments/Experiment06_DeduplicationStrategies.scala) | dropDuplicates, window, aggregation | dropDuplicates (non-deterministic) | row_number window / struct-max trick |
| 07 | [Shuffle Partition Tuning](../src/main/scala/com/ledgerforge/experiments/Experiment07_ShufflePartitionTuning.scala) | shuffle.partitions, coalescing | Fixed 200 partitions | Tuned values / AQE auto-coalesce |
| 08 | [UDF vs Native Expressions](../src/main/scala/com/ledgerforge/experiments/Experiment08_UDFvsNative.scala) | UDF, WholeStageCodegen, Catalyst | Scala UDF | Native when/otherwise |

---

## Concepts Covered by Each Experiment

| Concept | Experiments |
|---------|-------------|
| Shuffle behaviour | 01, 02, 05, 07 |
| Join strategies (SMJ, BHJ, SHJ) | 01, 02, 05 |
| Data skew | 01, 05 |
| Partition pruning | 03 |
| Caching & lineage | 04 |
| AQE runtime optimization | 05, 07 |
| Deduplication patterns | 06 |
| WholeStageCodegen | 08 |
| Physical plan analysis (`explain`) | ALL |
| Spark UI interpretation | ALL |

---

## What to Observe in Spark UI

### Stages Tab
- **Task count** = `spark.sql.shuffle.partitions` for each shuffle stage.
- **Task duration distribution** — look for stragglers (skew indicator).
- **Shuffle read/write** — data moved between stages.
- **GC time** — high GC = partitions too large.

### SQL Tab
- **Physical plan** — look for `Exchange` (shuffle), `BroadcastExchange`,
  `SortMergeJoin`, `BroadcastHashJoin`, `AdaptiveSparkPlan`.
- **WholeStageCodegen(*)** — everything inside this node runs as generated
  Java code. UDFs break this boundary.
- **PartitionFilters** — shows which filters were pushed to the file scan.

### Storage Tab
- **Cached RDDs** — shows memory/disk usage for `.cache()` and `.persist()`.
- **Size in memory vs. on disk** — shows the serialisation overhead.

### Jobs Tab
- **Job count** — without caching, the same DAG appears multiple times.
  With caching, only the first action triggers a full DAG execution.

---

## Dataset Characteristics (Important for Experiments)

| Property | Value | Impact |
|----------|-------|--------|
| Transaction count | ~50M rows | Large enough to observe real performance differences |
| Account ID distribution | Zipf (exponent=1.2) | ~0.1% of accounts hold ~30% of transactions → skew |
| Duplicate rate | ~2% | Same transaction_id, different ingest_date |
| Null rate | ~2% | Random nulls in key columns |
| Invalid value rate | ~3% | e.g., account_id = -1, currency = "XXX" |
| Partition column | event_date (30 days) | Enables partition pruning experiments |
| Dimension tables | product (5K), branch (500) | Small enough to broadcast |
| SCD2 customers | ~5M with version history | Temporal join challenges |

---

## Suggested Learning Path

1. **Start with Experiment 02 (Broadcast Joins)** — easiest to understand;
   clear difference in plans and timing.
2. **Experiment 03 (Partition Pruning)** — fundamental for production pipelines.
3. **Experiment 08 (UDF vs Native)** — important for writing efficient Spark code.
4. **Experiment 06 (Deduplication)** — common real-world pattern.
5. **Experiment 07 (Shuffle Partitions)** — tuning knob everyone should know.
6. **Experiment 04 (Caching)** — essential for multi-consumer pipelines.
7. **Experiment 01 (Skewed Joins)** — advanced; builds on join understanding.
8. **Experiment 05 (AQE)** — capstone; ties together shuffle, skew, and coalescing.

---

## Quick Reference: Key Spark Configs

| Config | Default | What It Does |
|--------|---------|-------------|
| `spark.sql.shuffle.partitions` | 200 | Number of partitions after each shuffle |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Max size of table to auto-broadcast |
| `spark.sql.adaptive.enabled` | true (3.2+) | Enable Adaptive Query Execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Merge small post-shuffle partitions |
| `spark.sql.adaptive.skewJoin.enabled` | true | Split skewed partitions in joins |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5 | Partition is skewed if N× larger than median |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256MB | Min size to consider a partition skewed |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1MB | Don't coalesce below this size |

---

## Related Problem Statements

These experiments provide the foundations needed for the full problem set in
[problem_statements.md](problem_statements.md). The mapping:

| Experiment | Related Problems |
|-----------|-----------------|
| 01 — Skewed Joins | Problem 11 (Salting) |
| 02 — Broadcast Joins | Problem 2 (Product Enrichment), Problem 15 (Plan Analysis) |
| 03 — Partition Pruning | Problem 7 (Late Data), Problem 10 (Small File), Problem 16 (Incremental) |
| 04 — Caching | Problem 14 (Caching Comparison) |
| 05 — AQE | Problem 13 (AQE Experiment) |
| 06 — Dedup | Problem 4 (Transaction Dedup) |
| 07 — Shuffle Partitions | Problem 10 (Partition Optimization) |
| 08 — UDF vs Native | Problem 9 (Risk Bucket) |

