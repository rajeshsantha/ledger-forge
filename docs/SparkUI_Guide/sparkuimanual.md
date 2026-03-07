# Spark UI — Complete Technical Manual

> **Audience:** Senior data engineers who know Spark well but want to deeply interpret every metric and graph in the Spark UI.  
> **Version:** Apache Spark 3.x (3.2–3.5)  
> **Purpose:** A reusable reference you can open alongside any production debugging session.

---

## Table of Contents

1. [Spark Execution Model](#section-1--spark-execution-model)
2. [Complete Tour of Spark UI Tabs](#section-2--complete-tour-of-spark-ui-tabs)
3. [How to Read the DAG](#section-3--how-to-read-the-dag)
4. [Exchange and Shuffle Internals](#section-4--exchange-and-shuffle-internals)
5. [Join Strategies](#section-5--join-strategies)
6. [Stage-Level Debugging](#section-6--stage-level-debugging)
7. [Adaptive Query Execution](#section-7--adaptive-query-execution)
8. [Performance Debugging Playbook](#section-8--performance-debugging-playbook)
9. [Optimization Cheat Sheet](#section-9--optimization-cheat-sheet)
10. [How Senior Engineers Use Spark UI](#section-10--how-senior-engineers-use-spark-ui)

---

## Section 1 — Spark Execution Model

Understanding the execution model is the foundation for everything you see in Spark UI. Without this mental model, every metric is noise.

### 1.1 The Transformation Pipeline

When you write DataFrame code, Spark does not execute it immediately. It builds a plan, optimizes it, then executes it lazily when an action is called.

```
Your DataFrame Code
        │
        ▼
   Unresolved Logical Plan        ← Parsed AST (column names not yet validated)
        │
        ▼  [Analyzer — resolves names against catalog]
   Resolved Logical Plan          ← Column types resolved, relations checked
        │
        ▼  [Catalyst Optimizer — applies rule-based & cost-based rewrites]
   Optimized Logical Plan         ← Predicates pushed down, projections pruned, joins reordered
        │
        ▼  [Planner — selects physical strategies]
   Physical Plan(s)               ← Multiple candidate plans
        │
        ▼  [Cost Model — picks cheapest]
   Selected Physical Plan         ← The plan Spark will actually execute
        │
        ▼  [WholeStageCodegen — JIT-compiles hot paths]
   Code-Generated Physical Plan
        │
        ▼  [Action triggers execution]
        │
   ┌────┴─────────────────────────────┐
   │              DAG                 │
   │  Job → Stages → Tasks            │
   │  (one per action call)           │
   └──────────────────────────────────┘
```

### 1.2 Key Concepts Defined

**Job**  
A Job is created every time Spark executes an *action* (`.collect()`, `.count()`, `.write`, `.show()`). One action = one Job. A single Spark application contains many jobs, one per action.

**Stage**  
A Stage is a set of tasks that can run in parallel without a shuffle. Stage boundaries are created wherever data must be redistributed across the network — i.e., wherever an `Exchange` (shuffle) node appears in the physical plan. Every `Exchange` node introduces a new stage boundary.

```
Stage 0: Scan + Filter + Project   (no shuffle)
         ──── shuffle write ────►
Stage 1: HashAggregate             (reads shuffled data)
         ──── shuffle write ────►
Stage 2: SortMergeJoin             (reads from both sides)
```

**Task**  
A Task is the smallest unit of execution. One task processes exactly one partition of data. All tasks in a stage run the same code on different partitions. Task count = partition count for that stage.

**Partition**  
A partition is a logical chunk of data. Spark processes one partition per task. The number of partitions controls parallelism. After a shuffle, partition count is controlled by `spark.sql.shuffle.partitions` (default 200) unless AQE coalesces them.

**Shuffle**  
A shuffle is the process of redistributing data across executors by key. It involves:
- **Shuffle Write** — map-side: tasks write sorted, partitioned output to local disk
- **Shuffle Read** — reduce-side: tasks fetch remote blocks from every map task

Shuffles are expensive because they involve disk I/O, serialization, and network transfer.

**DAG (Directed Acyclic Graph)**  
The DAG is the dependency graph of stages. It shows which stages must complete before others can start. The Spark UI renders the DAG as a visual graph in the SQL tab and as stage dependency arrows in the Stages tab.

### 1.3 Catalyst Optimizer

Catalyst is Spark's query optimization framework. It applies transformations in two passes:

**Rule-Based Optimization (RBO)** — always applied:
- Predicate pushdown (push filters close to the scan)
- Projection pruning (read only referenced columns)
- Constant folding (`1 + 1` → `2` at plan time)
- Boolean simplification

**Cost-Based Optimization (CBO)** — applied if statistics are available (`ANALYZE TABLE`):
- Join reordering (put smaller table on the build side)
- Join strategy selection based on estimated sizes

You can inspect what Catalyst did by calling `.explain(true)` on any DataFrame, which prints all four plan levels.

### 1.4 Adaptive Query Execution (AQE)

AQE is Spark's runtime optimizer (Spark 3.0+, enabled by default in 3.2+). Unlike Catalyst, which optimizes before execution, AQE re-optimizes the plan *between shuffle stages* based on actual runtime statistics.

AQE can:
- Coalesce small post-shuffle partitions into larger ones
- Split skewed partitions automatically
- Switch a SortMergeJoin to a BroadcastHashJoin at runtime if one side turns out to be small

When AQE is active, the physical plan is wrapped in an `AdaptiveSparkPlan` node in the SQL tab, and you will see "re-optimized plan" details after stages complete.

---

## Section 2 — Complete Tour of Spark UI Tabs

### 2.1 Jobs Tab

**What it shows:** Every action that has triggered a Job, its status, how many stages it has, and overall task progress.

**Key columns:**

| Column | Meaning |
|--------|---------|
| Job Id | Auto-incrementing integer. Higher = later in time. |
| Description | The action call site: file name + line number. Click to see the job's DAG. |
| Submitted | Wall-clock time the action was triggered. |
| Duration | Elapsed wall time (not CPU time). |
| Stages: Succeeded/Total | e.g., `2/4` means 2 of 4 stages complete. The other 2 are pending or active. |
| Tasks (all stages): Succeeded/Total | Fine-grained progress. The blue progress bar fills as tasks complete. |

**How to use it:**
- If a job has been running for a long time, click the Job ID to drill into its stages.
- Compare job durations between baseline and optimized runs.
- Multiple completed jobs with very similar descriptions often indicate a missing `.cache()` (the same DAG is recomputed for each action).

**Common mistakes:**
- Confusing job duration with stage duration — a job is only as fast as its slowest stage.
- Ignoring the "Active Jobs" section while a job is running — this is where you catch skew in real time.

---

### 2.2 Stages Tab

**What it shows:** Every stage across all jobs, with per-stage aggregate metrics.

**Key columns:**

| Column | Meaning |
|--------|---------|
| Stage Id | Unique integer per stage per application. |
| Description | The last operation before the stage boundary (e.g., `collect at ...`). |
| Submitted | Wall time the stage became ready to execute. |
| Duration | Wall time from first task to last task finishing. |
| Tasks: Succeeded/Total | e.g., `200/200`. If tasks are failing and retrying, you'll see mismatches. |
| Input | Data read from disk/HDFS/S3 in this stage. |
| Output | Data written to disk by this stage (for `write` operations). |
| Shuffle Read | Data read from shuffle files (from prior stages). |
| Shuffle Write | Data written to shuffle files (for next stages to read). |
| Spill (Memory) | Data that had to be spilled from JVM heap to off-heap. |
| Spill (Disk) | Data that had to be spilled to disk. |

**Interpreting the task distribution graph:**  
Click into a stage to see a histogram of task durations. A healthy stage shows a narrow bell curve. A skewed stage shows a long right tail — a few tasks taking 10–100× longer than the median.

**How to use it:**
- Sort by Duration descending to find your bottleneck stage.
- Look at Shuffle Write of one stage and Shuffle Read of the next — they should be similar in size. A large size ratio signals re-partitioning overhead.
- Spill (Disk) > 0 is a warning sign — partitions are too large for available memory.

**Common mistakes:**
- Ignoring the task distribution graph and only reading aggregate metrics.
- Not distinguishing Input (disk read) from Shuffle Read (network read).

---

### 2.3 SQL / DataFrame Tab

**What it shows:** Every DataFrame query plan, rendered as an interactive DAG with per-node metrics.

This is the most important tab for performance debugging. Every node in the plan shows:
- Number of rows output
- Time spent in that node
- Data size processed

**How to navigate:**
- Each query appears as a row. Click "Details" to expand the full physical plan as text, or click the query graph area to see the visual DAG.
- Nodes are colored by type (exchange, join, aggregate, scan).
- Hover over nodes to see row counts and timing.

**What to look for:**
- `Exchange` nodes = shuffles. Minimize these.
- `BroadcastExchange` = broadcast join. Confirm it appears for small tables.
- Row count explosions between nodes = data explosion (e.g., a cross join happening accidentally).
- Row count collapse = heavy filtering — ensure it happens early (close to `Scan`).

**How to use it:**
- After an optimization, compare the old plan (more Exchanges) with the new plan (fewer Exchanges).
- Check that `PartitionFilters` appears in Scan nodes when you filter on partition columns.
- Look for `AdaptiveSparkPlan` wrapping the whole query when AQE is enabled.

---

### 2.4 Storage Tab

**What it shows:** All DataFrames and RDDs that have been cached with `.cache()` or `.persist()`.

**Key columns:**

| Column | Meaning |
|--------|---------|
| RDD Name | The DataFrame or RDD identifier. |
| Storage Level | MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, etc. |
| Cached Partitions | How many partitions are currently materialized. |
| Fraction Cached | What % of the total partitions are cached (1.0 = 100%). |
| Size in Memory | Bytes held in JVM heap. |
| Size on Disk | Bytes spilled or persisted to disk. |

**How to use it:**
- A "Fraction Cached" less than 1.0 means some partitions couldn't fit in memory and were evicted or not yet materialized.
- Compare "Size in Memory" vs. "Size on Disk" to measure serialization overhead.
- If a cached DataFrame shows as 0 partitions, the cache hasn't been materialized yet (lazy evaluation — the first action on it will populate it).

**Common mistakes:**
- Forgetting to call `.unpersist()` — old cached DataFrames consume memory across jobs.
- Caching too early in a pipeline (caching un-filtered data wastes memory).

---

### 2.5 Executors Tab

**What it shows:** Resource usage and task statistics broken down per executor process.

**Key columns:**

| Column | Meaning |
|--------|---------|
| Executor ID | `driver` for the driver process; integers for worker executors. |
| Address | Host:port of the executor JVM. |
| State | Active / Dead. Dead executors were lost (OOM, timeout, eviction). |
| RDD Blocks | Number of cached RDD partitions on this executor. |
| Storage Memory | Used/Total memory for caching. |
| Disk Used | Disk space used for spill or cache. |
| Cores | Number of task slots. |
| Active Tasks | Currently executing tasks. |
| Failed Tasks | Tasks that failed and were retried. |
| Complete Tasks | Total successfully completed tasks. |
| Task Time (GC Time) | Total CPU time spent in tasks, with GC portion shown. GC > 10% of task time is a red flag. |
| Input | Data read from external storage. |
| Shuffle Read / Write | Network I/O for shuffles. |

**How to use it:**
- Look for imbalanced shuffle read/write across executors — a sign of data skew.
- High GC Time on specific executors = those executors are holding oversized partitions.
- Dead executors mid-job = OOM or external failure.
- Check that task counts are balanced across executors. One executor doing 10× more tasks than others is a scheduler or skew problem.

---

### 2.6 Environment Tab

**What it shows:** JVM properties, Spark configuration, classpath entries, and system properties.

**How to use it:**
- Verify `spark.sql.shuffle.partitions` is set to your tuned value, not the default 200.
- Confirm `spark.sql.adaptive.enabled = true`.
- Check `spark.sql.autoBroadcastJoinThreshold` — if set to -1, broadcasts are disabled.
- Verify memory configs: `spark.executor.memory`, `spark.executor.memoryOverhead`.
- Cross-reference your `spark-submit` configs with what's actually active.

**Common mistakes:**
- Assuming configs are set correctly without verifying here. Operator-level configs can override application-level settings.

---

## Section 3 — How to Read the DAG

The DAG in the SQL tab is read **bottom to top** — data flows upward. Each node represents one physical operation.

```
        [Result to Driver]
               ▲
        [HashAggregate]   ← Final aggregation
               ▲
        [Exchange]        ← Shuffle (stage boundary)
               ▲
        [HashAggregate]   ← Partial aggregation (pre-shuffle)
               ▲
        [Filter]          ← Predicate filter
               ▲
        [FileScan]        ← Read from Parquet/ORC/etc.
```

### 3.1 Node Reference

**Scan / FileScan**  
Reads data from external storage. Shows filename pattern, schema, and — critically — `PartitionFilters` and `PushedFilters`. If your `WHERE` clause filter appears under `PushedFilters`, Catalyst has pushed it to the file reader (good). If it doesn't appear there and instead appears as a separate `Filter` node above the scan, the pushdown failed.

**Filter**  
Applies a boolean predicate to rows. If this appears above a Scan node instead of inside it (as a pushed filter), data is being read in full then filtered — wasteful for large datasets.

**Project**  
Selects a subset of columns. Normally appears as column pruning — Spark only reads referenced columns from columnar formats like Parquet. If you see many columns in a Project, check whether you've selected too broadly upstream.

**Exchange (Shuffle)**  
The most important node to find. Every Exchange means: write data to local disk, send it across the network, read it on the other side. Exchanges are stage boundaries. Two types:
- `Exchange hashpartitioning(key, N)` — shuffle by hash of `key` into `N` partitions (SortMergeJoin, groupBy)
- `Exchange SinglePartition` — collect everything to one partition (bad for large data — this is an `.orderBy()` on a large DataFrame)
- `BroadcastExchange` — ship a small table to all executors

**HashAggregate**  
Aggregation executed as a two-phase hash map. You'll usually see two HashAggregate nodes: one before an Exchange (partial aggregation) and one after (final merge). The pre-shuffle partial agg dramatically reduces shuffle volume.

**SortMergeJoin (SMJ)**  
The default join for large tables. Both sides are shuffled by join key, sorted, then merged in a single pass. Expensive because it requires two shuffles (one per side) and sorting. You'll see two Exchange nodes feeding into a Sort feeding into SortMergeJoin.

**BroadcastHashJoin (BHJ)**  
A small table is shipped to every executor via a `BroadcastExchange`. The large table is scanned locally and probed against the broadcast hash table. Zero shuffle for the large side — extremely fast. Only feasible when the small table fits in executor memory (default threshold: 10MB, tune with `spark.sql.autoBroadcastJoinThreshold`).

**ShuffleHashJoin (SHJ)**  
Shuffles both sides by key (like SMJ) but builds a hash table instead of sorting. Faster than SMJ when one side is much smaller but doesn't fit for broadcast. Disabled by default in recent Spark versions because it can OOM on large inputs.

**Sort**  
Sorts rows within a partition. Appears before SortMergeJoin (each side must be sorted) and before Window functions. Also appears when you call `.orderBy()`.

**Window**  
Window function computation (`row_number()`, `rank()`, `lag()`, etc.). Requires a shuffle (Exchange) to group rows by the `PARTITION BY` key and a sort within each partition. Window functions are expensive — always check they're truly necessary and that the PARTITION BY key has good cardinality.

**Union**  
Combines multiple DataFrames with the same schema. Creates a no-shuffle merge of partitions. Appears as a node with multiple inputs in the DAG.

**Repartition**  
An explicit `df.repartition(N, col)` call. Produces an Exchange node. Forces a full shuffle — use only when partition count or key distribution is genuinely wrong for downstream operations.

**Coalesce**  
Reduces partition count without a full shuffle (only moves data from excess partitions). Cheaper than Repartition. In the DAG this may appear as `Coalesce` or, when AQE is active, as `AQEShuffleRead` with coalesced partition counts.

**Limit**  
`LIMIT N` applied at plan level. When appearing before a shuffle, it reduces data volume efficiently. After a shuffle, it may still require all shuffle writes to complete.

### 3.2 WholeStageCodegen

`WholeStageCodegen` is a wrapper node in the physical plan (marked with an asterisk `*` in the explain output). Everything inside this node has been compiled into a single JVM bytecode method — Spark fuses multiple operations into tight loops, eliminating virtual dispatch and object allocation overhead.

UDFs break `WholeStageCodegen` because Spark cannot inline opaque Scala/Python functions into the generated code. This is why native expressions (`when/otherwise`, `concat`, built-in functions) outperform equivalent UDFs — they stay inside the codegen boundary.

**ColumnarToRow / RowToColumnar**  
These nodes appear when switching between columnar execution (used by some connectors and Photon) and the standard row-based Volcano model. Frequent transitions are overhead — try to keep columnar operations together.

---

## Section 4 — Exchange and Shuffle Internals

### 4.1 How a Shuffle Works

```
Map Phase (Stage N)                    Reduce Phase (Stage N+1)
┌──────────────────────────┐           ┌──────────────────────────┐
│ Task 0                   │           │ Task 0 (partition 0)     │
│  writes partition 0 data ├──────────►│  reads from ALL map tasks│
│  writes partition 1 data │           └──────────────────────────┘
│  writes partition 2 data │           ┌──────────────────────────┐
└──────────────────────────┘           │ Task 1 (partition 1)     │
                                       │  reads from ALL map tasks│
┌──────────────────────────┐           └──────────────────────────┘
│ Task 1                   │
│  writes partition 0 data ├───────────────────► (same)
│  writes partition 1 data │
│  writes partition 2 data │
└──────────────────────────┘
```

Every map task writes one output file per reduce partition. With 200 map tasks and 200 reduce partitions, that's 40,000 small files on disk — this is why shuffle I/O is so expensive.

### 4.2 Shuffle Write

Shuffle write happens in the map stage. The map task:
1. Computes its rows
2. Hashes each row's key to determine target partition
3. Sorts rows by partition (and optionally by key within partition)
4. Writes sorted data to a local shuffle file
5. Writes an index file mapping partition → byte offset

**Metrics to watch:**
- **Shuffle Write Size** per task: should be roughly equal across tasks (balanced). If one task writes 100× more than others, that key is skewed.
- **Shuffle Write Time**: time to hash, sort, and write locally. Usually small compared to the actual computation.

### 4.3 Shuffle Read

Shuffle read happens in the reduce stage. Each reduce task:
1. Fetches its partition's data blocks from every map task's output
2. Merges and sorts the fetched blocks
3. Processes the sorted data

**Remote blocks** = data fetched from other executors (network transfer).  
**Local blocks** = data fetched from the same executor's local disk (no network, but still disk I/O).

**Metrics to watch:**
- **Shuffle Read Size** per task: large imbalance = skew.
- **Remote Bytes Read**: high value relative to local = lots of network transfer.
- **Fetch Wait Time**: time the reduce task spent waiting for shuffle blocks. High fetch wait = network bottleneck or slow shuffle service.

### 4.4 Shuffle Spill

When a shuffle task's in-memory buffer fills up before it can write to disk, Spark spills the buffer to a temporary spill file. This adds extra disk I/O (write spill, then read spill, then write final output).

**Spill (Memory)** in the Stages tab = amount of data that was in memory before spilling.  
**Spill (Disk)** = amount of data written to the spill file.

If you see consistent spill across many tasks, the partition size is too large for available executor memory. Remedies:
- Increase `spark.sql.shuffle.partitions` to reduce partition size
- Increase `spark.executor.memory`
- Enable AQE to auto-coalesce (reducing partition count won't help spill — increase it instead)

### 4.5 Shuffle Compression

Spark compresses shuffle data by default (`spark.shuffle.compress = true`, using LZ4). This reduces network and disk I/O at the cost of CPU. For CPU-bound jobs on fast networks, consider disabling compression.

### 4.6 Detecting Shuffle Bottlenecks

In the SQL tab, count the `Exchange` nodes. Each Exchange is a shuffle. Ask:
1. Is this shuffle necessary? Can it be eliminated by broadcasting, pre-partitioning, or restructuring the query?
2. Is the shuffle balanced? Check shuffle read size per task in the Stages tab.
3. Is the shuffle spilling? Check Spill (Disk) in the Stages tab.

---

## Section 5 — Join Strategies

### 5.1 BroadcastHashJoin (BHJ)

**When Spark chooses it:** One side of the join is estimated below `spark.sql.autoBroadcastJoinThreshold` (default 10MB). With AQE enabled, Spark can upgrade to BHJ at runtime if the actual size is below threshold.

**How it works:**
1. The small side is collected by the driver
2. The driver broadcasts a hash table to every executor
3. The large side is scanned locally and each row is probed against the local hash table
4. No shuffle of the large side

**Performance:** Fastest join strategy. Zero shuffle for the large side. The only overhead is broadcasting the small table (once per executor).

**How to force it:**
```scala
import org.apache.spark.sql.functions.broadcast
largeDF.join(broadcast(smallDF), "key")
```

**How to detect it in UI:** Look for `BroadcastHashJoin` and `BroadcastExchange` in the SQL plan. If you expected BHJ but see `SortMergeJoin`, the small table was too large or AQE wasn't enabled.

### 5.2 SortMergeJoin (SMJ)

**When Spark chooses it:** Both tables are large (neither qualifies for broadcast). Default for large-large joins.

**How it works:**
1. Both sides are shuffled by the join key (two Exchange nodes)
2. Both sides are sorted within each partition
3. A merge step walks both sorted streams simultaneously

**Performance:** Expensive — requires two shuffles and sorting. But robust and memory-efficient (doesn't build a hash table).

**How to detect it in UI:** `SortMergeJoin` node with two `Exchange` nodes as inputs, each fed by a `Sort`.

### 5.3 ShuffleHashJoin (SHJ)

**When Spark chooses it:** Manually enabled via `spark.sql.join.preferSortMergeJoin = false`. One side is smaller but too large to broadcast.

**How it works:** Shuffles both sides, then builds a hash table from the smaller side. No sorting required. Faster than SMJ for moderately sized tables.

**Risk:** Can OOM if the hash table doesn't fit in memory. Spark 3.x prefers SMJ by default for safety.

### 5.4 CartesianJoin

**When Spark chooses it:** A join with no join condition (cross join) or when explicitly specified. Also appears accidentally when join conditions are written incorrectly.

**Performance:** O(M × N) rows output. Catastrophically expensive for large inputs. If you see `CartesianProduct` in the SQL plan unexpectedly, your join condition is wrong or missing.

### 5.5 Join Strategy Decision Tree

```
Is the small side < autoBroadcastJoinThreshold?
  YES → BroadcastHashJoin
  NO  →
    Is preferSortMergeJoin = false AND small side fits in hash table?
      YES → ShuffleHashJoin
      NO  → SortMergeJoin
```

---

## Section 6 — Stage-Level Debugging

### 6.1 Detecting Data Skew

Click into a Stage and look at the task duration histogram. Skew manifests as:
- Median task time: 2 seconds
- 95th percentile: 2 seconds
- Max task time: 4 minutes → **skewed**

Also look at shuffle read size per task. If 199 tasks read 50MB each and 1 task reads 50GB, the key is skewed.

**Signals of skew:**
- One or a few tasks running dramatically longer than others (straggler tasks)
- High max shuffle read vs. low median shuffle read
- Stage overall duration is dominated by one task
- In the Executors tab, one executor has 10× more data processed

### 6.2 Slow Tasks and Stragglers

A straggler is any task that takes significantly longer than the median. Causes:
- **Data skew** — the task's partition has far more data
- **GC pressure** — the task's JVM heap is full, causing long GC pauses
- **Spill** — the task is writing to disk because its partition is too large
- **Remote shuffle fetch** — the task is waiting for shuffle blocks from slow nodes
- **Speculative execution** — if enabled, Spark will re-launch slow tasks on other executors (see speculative task count in stage detail)

### 6.3 Key Stage Metrics

| Metric | Healthy Range | Problem Signal |
|--------|---------------|----------------|
| Task duration spread | Narrow histogram | Long right tail (skew) |
| GC time / Task time | < 5% | > 10% means heap pressure |
| Spill (Disk) | 0 | Any value means OOM pressure |
| Shuffle Read Size (max/median) | Ratio ≈ 1–3× | Ratio > 10× means skew |
| Failed Tasks | 0 | Any failures mean instability |
| Fetch Wait Time | < 1s | High value means network bottleneck |

### 6.4 Executor Imbalance

Even without skew, executors can be imbalanced because of:
- Uneven partition counts assigned to executors (scheduling)
- Caching on specific executors causing more local reads
- Dead/degraded executors receiving fewer tasks

Check the Executors tab and compare Task Time across executors. They should be roughly proportional to core count.

---

## Section 7 — Adaptive Query Execution

AQE (enabled by default in Spark 3.2+) adds three major runtime optimizations:

### 7.1 Coalescing Shuffle Partitions

**Problem:** You set `spark.sql.shuffle.partitions = 200`, but after a filter that removes 99% of data, the remaining 200 partitions are 1KB each. 200 tiny tasks is wasteful.

**AQE solution:** After each shuffle completes, AQE inspects actual partition sizes and merges small adjacent partitions into larger ones.

**In the UI:** The SQL plan shows `AQEShuffleRead` instead of a regular `Exchange`. The number of tasks in the post-shuffle stage will be less than `spark.sql.shuffle.partitions`.

**Config:**
```
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.coalescePartitions.minPartitionSize = 1MB
```

### 7.2 Skew Join Optimization

**Problem:** One partition in a SortMergeJoin is 50GB while others are 50MB. The task processing that partition runs for 30 minutes.

**AQE solution:** AQE detects skewed partitions (those larger than `skewedPartitionFactor × median` and larger than `skewedPartitionThresholdInBytes`) and splits them into multiple sub-tasks. The other side of the join for those keys is read multiple times.

**In the UI:** The SQL plan shows `SkewJoin=true` on the SortMergeJoin node. The skewed stage has more tasks than `shuffle.partitions` (because some partitions were split).

**Config:**
```
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
```

### 7.3 Dynamic Broadcast Join

**Problem:** The query optimizer estimates a table as 500MB (too large to broadcast) but the actual data after filtering at runtime is only 5MB.

**AQE solution:** After the build side of a join shuffles, AQE measures the actual size. If it's below the broadcast threshold, AQE converts the SortMergeJoin to a BroadcastHashJoin at runtime.

**In the UI:** The physical plan initially shows `SortMergeJoin`, but after execution the plan is updated to show `BroadcastHashJoin`. You'll see `AdaptiveSparkPlan` at the root with a "re-optimized plan" section.

### 7.4 Reading the AQE Plan in the UI

When AQE is active, the SQL tab shows:
- `AdaptiveSparkPlan` wrapping the entire plan
- Before completion: the initial plan (pre-runtime statistics)
- After completion: the final executed plan (post-optimization)

Always read the **final executed plan** — the initial plan may not reflect what Spark actually did.

---

## Section 8 — Performance Debugging Playbook

### Symptom: Slow Join

1. Open SQL tab → find the query → look at the join node type
2. Is it `SortMergeJoin` when one side is small? → Force broadcast with `broadcast()` hint
3. Is it `BroadcastHashJoin` but still slow? → The broadcast table may be too large, causing driver OOM
4. Count the Exchange nodes → can any join be eliminated by pre-partitioning?
5. Check AQE — if enabled, did it upgrade to BHJ at runtime?

### Symptom: Skewed Key

1. Go to Stages tab → find the long-running stage
2. Click into it → look at task duration histogram → identify the straggler count
3. Check shuffle read size per task → confirm the straggler reads far more data
4. Solutions:
   - Enable AQE skew join optimization
   - Manually salt the skewed key (add a random integer 0–N to the key, duplicate the other side N times)
   - Filter out the hot key and process it separately

### Symptom: Too Many Shuffle Partitions

1. Go to SQL tab → count Exchange nodes
2. After each Exchange, check the stage's task count and per-task data size
3. If tasks are processing < 1MB each → too many partitions → reduce `spark.sql.shuffle.partitions` or enable AQE coalescing
4. If AQE is enabled and partitions are still tiny → check `minPartitionSize` config

### Symptom: Missing Broadcast Join

1. SQL tab → join is showing as `SortMergeJoin`
2. Check `spark.sql.autoBroadcastJoinThreshold` in Environment tab
3. If it's -1, broadcasts are disabled — re-enable or use `broadcast()` hint
4. If the threshold is set but BHJ isn't happening, the optimizer's size estimate is too high — use `ANALYZE TABLE` to update statistics or force with hint
5. If AQE is enabled and still no BHJ, the actual runtime size may genuinely exceed the threshold

### Symptom: Partition Pruning Not Working

1. SQL tab → click the FileScan node
2. Look for `PartitionFilters` — if empty, your filter was not pushed to the scan
3. Common causes:
   - Filtering on a non-partition column (obvious but worth checking)
   - Filtering via a UDF (Catalyst can't push opaque functions)
   - Filtering after a join (the filter must reference the partitioned table directly)
   - Dynamic partition pruning (DPP) not triggering — requires AQE and specific join patterns

### Symptom: Slow Parquet Scan

1. SQL tab → FileScan node → check records read vs. expected records
2. If reading far more records than expected, predicate pushdown failed — check PushedFilters
3. Check column pruning — is Project above FileScan selecting only needed columns?
4. Large number of small files? The scan is metadata-bound. Compact files first.
5. Missing statistics? Run `ANALYZE TABLE` to enable CBO to optimize scans.

### Symptom: Executor GC Pressure

1. Executors tab → check GC Time column
2. If GC Time > 10% of Task Time on specific executors → those executors hold oversized partitions
3. Solutions:
   - Increase partition count to reduce per-partition size
   - Increase `spark.executor.memory`
   - Tune `spark.memory.fraction` and `spark.memory.storageFraction`
   - Check if large objects are being collected in task closures (avoid collecting large arrays)

---

## Section 9 — Optimization Cheat Sheet

### Detect Skew Quickly
```
Stages tab → click into slow stage → 
task duration histogram → look for right tail →
sort tasks by "Shuffle Read Size" descending →
if top task >> median task, skew is confirmed
```

### Estimate Optimal Shuffle Partitions
```
Target: 100MB–200MB per partition after shuffle
Optimal partition count ≈ (total shuffle write size) / (target partition size)
Example: 100GB shuffle write → 100GB / 128MB = ~800 partitions
Set: spark.sql.shuffle.partitions = 800
Or: enable AQE to auto-coalesce
```

### Detect Unnecessary Shuffles
```
SQL tab → count Exchange nodes
Ask for each Exchange:
  - Is there a join? Can one side be broadcast?
  - Is there a groupBy? Is partial aggregation happening before the Exchange?
  - Is there a repartition() call that could be removed?
  - Is the sort necessary? (orderBy on large data = SinglePartition Exchange)
```

### Detect Inefficient Joins
```
SQL tab:
  - SortMergeJoin + one small side → force broadcast
  - CartesianProduct → fix missing join condition
  - Multiple SMJs on same key → consider pre-partitioning data by that key
  - BroadcastHashJoin but slow → broadcast table may be causing driver pressure
```

### Detect Bad Partitioning
```
Stages tab:
  - Too many tasks, each processing < 1MB → too many partitions → coalesce or reduce shuffle.partitions
  - Few tasks, each processing > 1GB → too few partitions → repartition or increase shuffle.partitions
  - Straggler tasks → skewed partitioning → salt or AQE skew join
```

### Detect Missing Predicate Pushdown
```
SQL tab → FileScan node → expand node →
check "PushedFilters" list →
if your WHERE condition isn't listed there, it's not pushed →
also check "PartitionFilters" for partition column filters
```

---

## Section 10 — How Senior Engineers Use Spark UI

A senior Spark engineer never looks at Spark UI randomly. They follow a deliberate workflow:

**Step 1 — Establish the baseline.** Before any optimization, run the job and record: total job duration, stage durations, key exchange sizes, task count per stage, and any spill. Screenshot or log these numbers.

**Step 2 — Start with the SQL tab.** Find the query for the slow operation. Look at the physical plan as text (use `.explain("formatted")` in code too). Count Exchanges. Identify join types. Check for expected optimizations (BHJ, partition filters, codegen markers).

**Step 3 — Find the bottleneck stage.** Go to Stages tab. Sort by Duration descending. Click into the slowest stage. Look at the task duration histogram. Is it data skew? GC? Spill? The histogram tells you.

**Step 4 — Correlate stage to plan.** Once you've found the slow stage, go back to the SQL tab and find which physical plan node corresponds to that stage. Now you know exactly which operation is slow and why.

**Step 5 — Check executors for systemic issues.** Executors tab: are all executors healthy? Is GC high? Is memory pressure causing evictions? Are some executors processing far more data?

**Step 6 — Form a hypothesis and apply one change.** Never apply multiple optimizations simultaneously — you won't know which one helped. Apply the most impactful change (e.g., enable AQE, force broadcast, increase shuffle partitions) and re-run.

**Step 7 — Compare plans, not just times.** After optimization, go to the SQL tab and confirm the plan changed as expected. A 2× speedup from a plan that's still wrong is lucky — a plan that's structurally correct will scale.

**Step 8 — Validate at scale.** A local test with 1M rows proves nothing. Re-run the optimized job on production data (or a representative sample) and verify the improvements hold.

**The mental model senior engineers carry:**  
Every second of job time is spent doing one of: reading data, computing (CPU), writing shuffle, waiting for network, or waiting for GC. The Spark UI makes all of these visible. The job of performance engineering is to systematically eliminate the biggest bottleneck, then find the next one.

---

*End of Spark UI Complete Technical Manual*  
*For the companion experiment playbook, see `sparkui_experiment_playbook.md`*
