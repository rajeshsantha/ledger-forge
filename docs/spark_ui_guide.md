# Spark UI — Complete Guide to Reading & Debugging Performance

> Based on your Experiment05_AQE run against 50M transactions + 5M accounts.
> Spark UI: http://localhost:4040

---

## 1. The Five Tabs — What Each One Tells You

```
┌──────┬────────┬─────────┬─────────────┬────────────┬──────────────────┐
│ Jobs │ Stages │ Storage │ Environment │ Executors  │ SQL / DataFrame  │
└──────┴────────┴─────────┴─────────────┴────────────┴──────────────────┘
```

| Tab | What It Shows | When to Use |
|-----|---------------|-------------|
| **Jobs** | One row per Spark "action" (count, collect, show, save) | See how many actions triggered, which are slow |
| **Stages** | Each job is split into stages at shuffle boundaries | Find the slow stage, compare task counts |
| **Storage** | Cached/persisted DataFrames, memory & disk usage | Verify caching is working, check memory pressure |
| **Environment** | All Spark configs, JVM args, classpath | Verify your config changes took effect |
| **Executors** | CPU, memory, GC time per executor | Find executor-level bottlenecks, GC pressure |
| **SQL / DataFrame** | Visual DAG of the physical plan with runtime metrics | **THE MOST IMPORTANT TAB** for performance tuning |

---

## 2. Jobs Tab — What You're Seeing

From your screenshot (Exp05_AQE_OFF):

```
Completed Jobs (2):
  Job 0: parquet at Experiment05_AQE.scala:142  — 3 s    (1/1 stages, 1/1 tasks)
  Job 1: parquet at Experiment05_AQE.scala:143  — 0.3 s  (1/1 stages, 1/1 tasks)

Active Jobs (1):
  Job 2: collect at Experiment05_AQE.scala:168  — 1.1 min (2/4 stages, 180/423 tasks)
```

### How to read this:

- **Jobs 0 & 1** — These are the `spark.read.parquet(...)` calls on lines 142-143.
  They are fast because Spark is just reading Parquet metadata (schema inference),
  not the actual data. Each is 1 stage with 1 task.

- **Job 2** — This is `pipeline.collect()` on line 168. This is the **real work**:
  reading 50M rows, joining, shuffling, aggregating, collecting results.
  - **4 stages** — this tells you there are shuffle boundaries in the plan
  - **423 total tasks** — spread across all stages
  - **180/423 succeeded** — it's still running when you took the screenshot

### Key insight:
> The number of **stages** = number of shuffle boundaries + 1.
> 4 stages means 3 Exchange (shuffle) operations in the physical plan.

---

## 3. Stages Tab — Finding the Bottleneck

From your screenshot (Exp05_AQE_ON):

```
Completed Stages (4):
  Stage 0: parquet — 0.2 s, 1/1 tasks          (schema inference)
  Stage 1: parquet — 79 ms, 1/1 tasks           (schema inference)
  Stage 2: collect — 12 s,  15/15 tasks, Input: 353.5 MiB, Shuffle Write: 547.6 MiB
  Stage 3: collect — 6 s,   8/8 tasks,  Input: 19.8 MiB,  Shuffle Write: 24.3 MiB

Active Stages (1):
  Stage 6: collect — 8 s, 2/7 tasks, Shuffle Read: 33.4 MiB, Shuffle Write: 39.0 KiB

Pending Stages (2):
  Stage 4: collect — 0/15 tasks
  Stage 5: collect — 0/8 tasks
```

### How to read each column:

| Column | Meaning |
|--------|---------|
| **Stage Id** | Sequential ID — higher = later in the DAG |
| **Description** | Which action + source code line triggered it |
| **Duration** | Wall-clock time for this stage |
| **Tasks: Succeeded/Total** | How many parallel tasks ran. Total = `shuffle.partitions` or number of input files |
| **Input** | Data read from disk (Parquet files) |
| **Output** | Data written to disk (if writing output) |
| **Shuffle Read** | Data read from previous stage's shuffle output |
| **Shuffle Write** | Data written as shuffle output for next stage |

### What the stages mean for your pipeline:

```
Your code:
  transactions.join(accounts.drop("branch_id"), "account_id")
              .groupBy("branch_id")
              .agg(sum("amount"), count(lit(1)))

DAG breakdown:
  Stage 0,1: Schema inference (parquet metadata reads)
  
  Stage 2: Scan transactions (50M rows, 353.5 MiB)
           → Shuffle Write 547.6 MiB (hash-partition by account_id for join)
           → 15 tasks (= number of parquet input files/partitions)

  Stage 3: Scan accounts (5M rows, 19.8 MiB)  
           → Shuffle Write 24.3 MiB (hash-partition by account_id for join)
           → 8 tasks (= number of parquet input files)

  Stage 6: Shuffle Read from Stage 2 + Stage 3
           → SortMergeJoin on account_id
           → groupBy(branch_id) + aggregate
           → 7 tasks (AQE coalesced from 200 → 7!)
           
  Stages 4,5: Pending (waiting for Stage 6 to complete)
```

### Key metrics to watch:

1. **Shuffle Write vs. Shuffle Read** — If write >> read, data is being shuffled
   to many partitions but only a few have data (wasteful with too many partitions).

2. **Task count with AQE** — Notice Stage 2 has 15 tasks, Stage 3 has 8 tasks,
   but Stage 6 has only 7 tasks. AQE coalesced the 200 shuffle partitions down
   to 7 because most were tiny/empty. Without AQE, this would be 200 tasks.

3. **Duration** — Stage 2 (12s) is the slowest because it scans 50M rows.
   Stage 3 (6s for 5M rows) is proportionally slower per-row because of the
   account table structure.

---

## 4. SQL / DataFrame Tab — THE MOST IMPORTANT VIEW

From your screenshot, this is the visual DAG with runtime metrics.

### Reading the DAG (top to bottom):

```
┌─────────────────────────┐     ┌─────────────────────────┐
│     Scan parquet        │     │     Scan parquet        │
│  240 files, 50M rows    │     │  8 files, 5M rows      │
│  894.4 MiB              │     │  85.6 MiB              │
│  30 partitions read     │     │                        │
│  (transactions)         │     │  (accounts)            │
└──────────┬──────────────┘     └──────────┬─────────────┘
           │                               │
  ┌────────▼────────────┐        ┌─────────▼────────────┐
  │ WholeStageCodegen(1)│        │ WholeStageCodegen(2) │
  │ ColumnToRow         │        │ ColumnToRow          │
  │ 50M rows → 12,240   │        │ 5M rows → 1,224     │
  │   input batches     │        │   input batches      │
  │                     │        │                      │
  │ Filter              │        │ Filter               │
  │ 50M → 49,001,213   │        │ 5M → 5,000,000      │
  │                     │        │                      │
  │ Project             │        └──────────┬───────────┘
  └──────────┬──────────┘                   │
             │                              │
    ┌────────▼──────────┐         ┌─────────▼──────────┐
    │    Exchange       │         │    Exchange        │
    │ shuffle write:    │         │ shuffle write:     │
    │ 49,001,213 records│         │ 5,000,000 records  │
    │ 547.6 MiB         │         │ 24.3 MiB           │
    └────────┬──────────┘         └─────────┬──────────┘
             │                              │
             └──────────┬───────────────────┘
                        │
               ┌────────▼──────────┐
               │  SortMergeJoin    │
               │  on account_id    │
               │                   │
               │  groupBy          │
               │  (branch_id)      │
               │                   │
               │  HashAggregate    │
               │  sum(amount),     │
               │  count(1)         │
               └────────┬──────────┘
                        │
               ┌────────▼──────────┐
               │    Exchange       │
               │  (shuffle for     │
               │   groupBy result) │
               └────────┬──────────┘
                        │
               ┌────────▼──────────┐
               │  HashAggregate    │
               │  (final merge)    │
               └───────────────────┘
```

### Key nodes explained:

#### **Scan parquet**
```
number of files read: 240          ← Spark read 240 parquet files (30 partitions × 8 files each)
number of output rows: 50,000,000  ← 50M rows in the transaction table
894.4 MiB                          ← total data size on disk
30 partitions read                 ← confirms all 30 event_date partitions were scanned
```
- **Performance tip**: If you only needed a few days, use `.filter(col("event_date").between(...))` 
  to trigger partition pruning (see Experiment 03). This would reduce files from 240 to ~24.

#### **WholeStageCodegen(N)**
```
WholeStageCodegen (1)
  duration: total (min, med, max (stageId: taskId))
  1.3 m (0 ms, 5.4 s, 5.8 s (stage 2.0: task 10))
```
- This means Spark generated Java bytecode for this pipeline segment.
- **Total time**: 1.3 minutes across all tasks.
- **Min/Med/Max task time**: 0ms / 5.4s / 5.8s — relatively even distribution.
- If max >> median, you have **skew** (one task is doing much more work).
- The `(stage 2.0: task 10)` tells you which specific task was the slowest.

#### **Filter**
```
number of output rows: 49,001,213
```
- Started with 50M rows, 49M passed the filter.
- The ~1M filtered rows are nulls/invalid account_ids from the `.drop("branch_id")` 
  and implicit null filtering in the join.

#### **Exchange (CRITICAL — This is a Shuffle)**
```
Exchange
  shuffle records written: 49,001,213
  shuffle write time total: 7.0 s (0 ms, 501 ms, 630 ms (stage 2.0: task 12))
  data size total: 1495.4 MiB (0.0 B, 105.9 MiB, 105.9 MiB (stage 2.0: task 4))
  
  records read: 35,286,657
  local bytes read total: 547.6 MiB (3.4 MiB, 45.9 MiB, 293.2 MiB (stage 6.0: task 26))
```

**This is the most important node for performance debugging.**

| Metric | What It Means |
|--------|--------------|
| **shuffle records written** | Number of rows sent to the shuffle |
| **shuffle write time** | Time spent serialising and writing shuffle data |
| **data size total** | Total bytes in the shuffle (1495.4 MiB ≈ 1.5 GB!) |
| **records read** | Rows read by the next stage from this shuffle |
| **local bytes read** | Data read from local shuffle files |
| **local merged chunks fetched: 0** | No remote shuffle (all local in local[*] mode) |

**Why records written (49M) ≠ records read (35M)?**
- Because this is the AQE_ON run — AQE may have coalesced partitions, and some 
  records are filtered/aggregated before the read completes.

**The skew indicator:**
```
local bytes read total: 547.6 MiB (3.4 MiB, 45.9 MiB, 293.2 MiB (stage 6.0: task 26))
                                    ↑ min     ↑ median   ↑ max
```
- **Max (293.2 MiB) is 6.4× the median (45.9 MiB)** — this confirms Zipf skew!
- Task 26 in Stage 6 is processing a "hot" account_id partition.
- This is exactly why Experiment 01 (salting) and Experiment 05 Config C (AQE skew 
  join) exist — to split that 293 MiB partition into smaller chunks.

#### **SortMergeJoin**
- Spark chose SortMergeJoin because both sides are too large (in its estimation) 
  for a BroadcastHashJoin.
- SortMergeJoin requires both sides to be sorted by the join key — that's why 
  there are Exchange nodes before it.

#### **HashAggregate (appears twice)**
```
First HashAggregate:  partial aggregation (per partition, before shuffle)
Second HashAggregate: final aggregation (after shuffle, merges partial results)
```
- Spark aggregates in two phases:
  1. **Partial** — each partition computes local sums/counts.
  2. **Final** — results are shuffled by group key (branch_id) and merged.
- This is like MapReduce: map-side combine + reduce-side merge.

---

## 5. Comparing AQE OFF vs AQE ON — What Changed

### From your screenshots:

| Metric | AQE OFF | AQE ON |
|--------|---------|--------|
| **App Name** | Exp05_AQE_OFF | Exp05_AQE_ON |
| **Job 2 tasks** | 180/423 (still running) | Fewer total tasks |
| **shuffle.partitions** | 200 (fixed) | 200 → coalesced to 7 |
| **Stage task counts** | 200 per shuffle stage | 15, 8, 7 (dynamic) |
| **Plan root** | Static plan | AdaptiveSparkPlan |

### AQE OFF (your first screenshot):
- Job 2 has **423 total tasks** across 4 stages.
- The shuffle stages have 200 tasks each (= `spark.sql.shuffle.partitions`).
- Many of these 200 tasks process empty or tiny partitions → wasted overhead.

### AQE ON (your second screenshot):
- Stage 6 has only **7 tasks** instead of 200.
- AQE looked at the actual shuffle output sizes and merged small partitions.
- Result: fewer tasks, less scheduling overhead, faster completion.

---

## 6. Performance Debugging Checklist

When you open Spark UI for any job, follow this checklist:

### Step 1: SQL Tab — Look at the DAG
```
□ Is there a BroadcastHashJoin or SortMergeJoin?
  → Small table (< 10MB)? Should be BroadcastHashJoin.
  
□ How many Exchange nodes? Each = one shuffle.
  → Minimize shuffles. Broadcast small tables. Pre-partition if possible.
  
□ Is WholeStageCodegen wrapping most nodes?
  → If broken, a UDF or incompatible operation is preventing codegen.
  
□ Check PartitionFilters in Scan nodes.
  → If empty and you expected pruning, your filter isn't on a partition column.
```

### Step 2: Stages Tab — Find the Slow Stage
```
□ Which stage has the longest duration?
  → Click into it for task-level details.
  
□ Compare Shuffle Write vs. Shuffle Read sizes.
  → Large shuffle = expensive. Can you reduce it with broadcast or filter?
  
□ Check task count vs. actual parallelism (cores).
  → 200 tasks on 8 cores = 25 waves. Might be wasteful if tasks are tiny.
```

### Step 3: Task Details (click a stage) — Find Skew
```
□ Look at the task duration distribution:
  - Summary Metrics: min / 25th / median / 75th / max
  
□ If max >> median (more than 3×), you have SKEW.
  → Example from your run: max=293 MiB, median=45.9 MiB = 6.4× skew!
  
□ Look at Shuffle Read Size per task:
  → Skewed tasks read much more data.
  
□ Look at GC Time per task:
  → High GC = partition too large for memory, causing spill.
```

### Step 4: Storage Tab — Verify Caching
```
□ Is the DataFrame you cached actually listed here?
  → If not, .cache() wasn't materialized (no action was called on it).
  
□ Check "Size in Memory" vs. "Size on Disk":
  → If most is on disk, memory is full — consider DISK_ONLY or repartitioning.
  
□ After .unpersist(), verify it disappears from this tab.
```

### Step 5: Environment Tab — Verify Config
```
□ Check spark.sql.adaptive.enabled = true/false (matches your experiment).
□ Check spark.sql.shuffle.partitions = 200 (or your custom value).
□ Check spark.sql.autoBroadcastJoinThreshold = 10485760 (10MB default).
```

---

## 7. Key Formulas for Performance Tuning

### Optimal shuffle partitions:
```
optimal_partitions = total_shuffle_data / target_partition_size

Example from your run:
  Shuffle data = 547.6 MiB (transaction side)
  Target = 128 MiB per partition
  Optimal = 547.6 / 128 ≈ 5 partitions

  AQE coalesced to 7 — close to optimal!
  Default 200 was 40× too many.
```

### Skew detection:
```
skew_factor = max_partition_size / median_partition_size

Your run: 293.2 MiB / 45.9 MiB = 6.4×
Spark AQE threshold: 5× (spark.sql.adaptive.skewJoin.skewedPartitionFactor)

6.4 > 5 → AQE would split this partition in Config C!
```

### Broadcast threshold:
```
If table_size < spark.sql.autoBroadcastJoinThreshold (10MB):
  → Spark auto-broadcasts (BroadcastHashJoin, no shuffle on large side)

Your account table: 85.6 MiB → too large for auto-broadcast.
Your product table: ~100 KB → auto-broadcast kicks in.
```

---

## 8. Red Flags in Spark UI

| What You See | Problem | Fix |
|-------------|---------|-----|
| Exchange node with 50M+ records | Unnecessary full shuffle | Broadcast the small side |
| max task time >> 10× median | Data skew | Salt the join key, or enable AQE skew join |
| 200 tasks but most finish in < 100ms | Too many partitions | Reduce `shuffle.partitions` or enable AQE coalescing |
| GC Time > 10% of task time | Partitions too large | Increase `shuffle.partitions` or add memory |
| No WholeStageCodegen | UDF or complex operation | Replace UDF with native expressions |
| PartitionFilters: [] | Partition pruning not working | Filter on the actual partition column |
| Scan reads ALL files | No predicate pushdown | Add `.filter()` before action |
| Multiple jobs for same DAG | DataFrame not cached | Add `.cache()` if used multiple times |

---

## 9. Mapping to Your Experiment05 Run

Your pipeline: `transactions.join(accounts, "account_id").groupBy("branch_id").agg(...)`

```
                              YOUR SPARK UI DAG
                              ==================

  [Scan: 50M txn rows, 894 MiB]     [Scan: 5M account rows, 85 MiB]
              │                                  │
  [WholeStageCodegen: Filter]        [WholeStageCodegen: Filter]
  49M rows pass                      5M rows pass
              │                                  │
  [Exchange: shuffle by account_id]  [Exchange: shuffle by account_id]
  Write: 49M records, 547 MiB        Write: 5M records, 24 MiB
              │                                  │
              └──────────┬───────────────────────┘
                         │
            [SortMergeJoin on account_id]     ← THIS IS THE BOTTLENECK
            Skew: max partition 293 MiB,
                  median partition 45.9 MiB
                         │
            [HashAggregate: partial]
            groupBy(branch_id), sum, count
                         │
            [Exchange: shuffle by branch_id]
            Write: much smaller (aggregated)
                         │
            [HashAggregate: final merge]
                         │
                    [CollectLimit]
                    Results to driver
```

### What AQE changed:
- **Without AQE**: 200 fixed tasks for the join stage → many empty partitions.
- **With AQE**: Coalesced to 7 tasks → each task has meaningful work.
- **With AQE + Skew**: Would additionally split the 293 MiB partition into ~6 
  sub-partitions of ~50 MiB each.

---

## 10. Quick Reference: Spark UI Shortcuts

| What to Check | Where |
|---------------|-------|
| Total job time | Jobs tab → Duration column |
| Shuffle data volume | Stages tab → Shuffle Read/Write columns |
| Task skew | Stage detail → Summary Metrics → compare min/median/max |
| Physical plan | SQL tab → click the query → visual DAG |
| Exchange count | SQL tab → count "Exchange" boxes in the DAG |
| Join strategy | SQL tab → look for "SortMergeJoin" or "BroadcastHashJoin" |
| AQE active? | SQL tab → root node says "AdaptiveSparkPlan" |
| Partition pruning | SQL tab → Scan node → "PartitionFilters" field |
| Codegen active? | SQL tab → "WholeStageCodegen" wrapping operations |
| Cache status | Storage tab → lists cached DataFrames |
| Config values | Environment tab → Spark Properties section |

