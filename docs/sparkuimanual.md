# Spark UI — Complete Technical Manual

> A comprehensive reference for understanding every aspect of Apache Spark UI.
> Written for data engineers who can write Spark code but want to master performance debugging.

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

### The Transformation Pipeline

When you write DataFrame code, Spark does not execute it immediately. It builds a plan, optimises it, and only executes when you call an **action**. Understanding this pipeline is the foundation for reading every element of Spark UI.

```
  Your Scala/Python Code
          │
          ▼
  ┌─────────────────────┐
  │   Unresolved         │   Column names, table references not yet validated
  │   Logical Plan       │
  └──────────┬──────────┘
             │  Analyzer (resolves names, types)
             ▼
  ┌─────────────────────┐
  │   Resolved           │   All columns, types, tables resolved
  │   Logical Plan       │
  └──────────┬──────────┘
             │  Catalyst Optimizer (rule-based + cost-based)
             ▼
  ┌─────────────────────┐
  │   Optimized          │   Predicate pushdown, constant folding,
  │   Logical Plan       │   column pruning, join reordering applied
  └──────────┬──────────┘
             │  Spark Planner (picks physical strategies)
             ▼
  ┌─────────────────────┐
  │   Physical Plan      │   Concrete algorithms chosen:
  │                      │   SortMergeJoin, BroadcastHashJoin, etc.
  └──────────┬──────────┘
             │  WholeStageCodegen (generates Java bytecode)
             ▼
  ┌─────────────────────┐
  │   DAG of RDDs        │   Directed Acyclic Graph of operations
  └──────────┬──────────┘
             │  DAGScheduler (splits at shuffle boundaries)
             ▼
  ┌─────────────────────┐
  │   Jobs → Stages      │   Each action = 1 Job. Shuffles = Stage boundaries.
  │   → Tasks            │   Each partition in a stage = 1 Task.
  └─────────────────────┘
```

### Core Concepts

#### Partition
A partition is a chunk of data that one task processes. It is the fundamental unit of parallelism. If your DataFrame has 200 partitions, up to 200 tasks can run in parallel (if you have 200 cores). The number of partitions comes from:
- **Input**: number of Parquet files, HDFS blocks, or Kafka partitions.
- **After shuffle**: controlled by `spark.sql.shuffle.partitions` (default 200).
- **Manual**: `repartition(N)` or `coalesce(N)`.

#### Task
A task is the smallest unit of work. One task processes one partition in one stage. A task runs on one core of one executor. If a stage has 200 partitions, it has 200 tasks. You observe individual task metrics in the Stages tab.

#### Stage
A stage is a group of tasks that can run without shuffling data. Spark creates a new stage at every **shuffle boundary** — that is, every `Exchange` node in the physical plan. Within a stage, all operations are pipelined (chained together without materialising intermediate results).

#### Job
A job is created every time you call an **action**: `count()`, `collect()`, `show()`, `save()`, `foreach()`. One action = one job. A job consists of one or more stages. If your code calls `df.count()` and then `df.show(5)`, that is two separate jobs.

#### DAG (Directed Acyclic Graph)
The DAG represents the lineage of operations. It flows from data sources (top) to the action (bottom). Each node is a transformation. Edges show data flow. The DAGScheduler inspects this graph and splits it into stages at shuffle boundaries.

#### Shuffle
A shuffle is the most expensive operation in Spark. It redistributes data across partitions — writing intermediate data to disk (shuffle write), then reading it across the network or locally (shuffle read). Shuffles occur during:
- `groupBy` / aggregation
- `join` (except broadcast join)
- `repartition`
- `orderBy` / `sort`
- Window functions with `partitionBy`

#### Catalyst Optimizer
Catalyst is Spark's rule-based and cost-based query optimizer. It transforms the logical plan into an optimized logical plan by applying rules like:
- **Predicate pushdown**: push filters closer to the data source.
- **Column pruning**: only read columns that are actually used.
- **Constant folding**: evaluate constant expressions at plan time.
- **Join reordering**: rearrange joins for efficiency.
- **Filter simplification**: merge and simplify filter conditions.

You see the result of Catalyst's work when you call `df.explain(true)` — the "Optimized Logical Plan" section shows what Catalyst produced.

#### Logical Plan
The logical plan describes **what** operations to perform without specifying **how**. It says "join these two DataFrames on column X" but does not say whether to use SortMergeJoin or BroadcastHashJoin. You can see this via `df.explain(true)` — the "Parsed Logical Plan" and "Analyzed Logical Plan" sections.

#### Physical Plan
The physical plan describes **how** to execute each operation. It has chosen concrete algorithms: `SortMergeJoin` instead of abstract "Join", `HashAggregate` instead of abstract "Aggregate". This is what the SQL/DataFrame tab in Spark UI visualises. You can see it via `df.explain()` or `df.explain(true)` — the "Physical Plan" section.

#### Adaptive Query Execution (AQE)
AQE is a runtime re-optimization framework (Spark 3.0+, enabled by default since 3.2). Unlike Catalyst which optimises at plan time with estimated statistics, AQE re-optimises **after** shuffle stages complete, using **actual** data sizes. It can:
1. Coalesce small shuffle partitions into larger ones.
2. Switch join strategy (e.g., convert SortMergeJoin to BroadcastHashJoin) if actual data is small.
3. Split skewed partitions into smaller sub-partitions during joins.

When AQE is active, the SQL tab shows `AdaptiveSparkPlan` as the root node, and the plan may change between stages.

---

## Section 2 — Complete Tour of Spark UI Tabs

Spark UI runs at `http://<driver-host>:4040` (default port). It has six tabs.

### Jobs Tab

**What it shows**: One row per Spark action (every call to `count`, `collect`, `show`, `write`, etc.).

| Column | Meaning |
|--------|---------|
| **Job Id** | Sequential ID, starting from 0 |
| **Description** | The action name + source code file and line number |
| **Submitted** | Timestamp when the action was called |
| **Duration** | Wall-clock time for the entire job |
| **Stages: Succeeded/Total** | How many stages the job required, and how many completed |
| **Tasks: Succeeded/Total** | Sum of all tasks across all stages |

**When engineers use it**:
- See how many actions your code triggered (too many = possible missing cache).
- Identify which action is slow by comparing durations.
- Quick overview of overall progress.

**Common mistakes**:
- Thinking Jobs 0/1 for `spark.read.parquet(...)` are expensive — they are just schema inference, usually < 3s.
- Confusing job count with stage count. One job can have many stages.

### Stages Tab

**What it shows**: Every stage across all jobs. Stages are grouped into Active, Pending, Completed, Failed, and Skipped.

| Column | Meaning |
|--------|---------|
| **Stage Id** | Sequential ID across all jobs |
| **Description** | Action + source code line that triggered this stage |
| **Duration** | Wall-clock time for this stage |
| **Tasks: Succeeded/Total** | Number of parallel tasks. Total = partitions in this stage |
| **Input** | Data read from disk (Parquet, CSV, etc.) |
| **Output** | Data written to disk (if this stage writes output) |
| **Shuffle Read** | Data read from previous stage's shuffle files |
| **Shuffle Write** | Data written to shuffle files for the next stage |

**Clicking into a stage** reveals the task-level detail page with:
- **Summary Metrics**: min, 25th percentile, median, 75th percentile, max for duration, GC time, shuffle read, shuffle write, spill.
- **Task table**: every individual task with its executor, duration, input/output sizes, and error messages.

**When engineers use it**:
- Find the slowest stage (the bottleneck).
- Detect data skew by comparing min/max task durations.
- Monitor shuffle volumes to find expensive shuffles.

**Common mistakes**:
- Ignoring "Skipped" stages — these mean Spark reused cached data (good, not a problem).
- Not clicking into the stage to see per-task metrics — the stage-level summary hides skew.

### SQL / DataFrame Tab

**What it shows**: A visual DAG of the physical plan with **runtime metrics** attached to each node. This is the single most informative tab for performance debugging.

Each query (triggered by an action) gets an entry. Click it to see:
- **Submitted Time, Duration**: overall query timing.
- **DAG visualization**: boxes for each physical operator with metrics.
- **Checkbox**: "Show the Stage ID and Task ID that corresponds to the max metric" — always enable this.

**Key nodes you'll see**: Scan, Filter, Project, Exchange, HashAggregate, SortMergeJoin, BroadcastHashJoin, BroadcastExchange, WholeStageCodegen, Sort, Window, CollectLimit.

**When engineers use it**:
- Count Exchange nodes (shuffles).
- Identify join strategies.
- Check row counts at each step to find where data explodes or shrinks.
- Verify partition pruning (look at `PartitionFilters` in Scan nodes).
- Detect codegen boundaries (WholeStageCodegen).

**Common mistakes**:
- Not enabling the "Show Stage ID and Task ID" checkbox — without it, you can't trace slow metrics to specific tasks.
- Only looking at the plan shape without reading the numeric metrics inside each node.

### Storage Tab

**What it shows**: All DataFrames/RDDs currently cached via `.cache()`, `.persist()`, or `.checkpoint()`.

| Column | Meaning |
|--------|---------|
| **RDD Name** | Internal name of the cached dataset |
| **Storage Level** | MEMORY_AND_DISK, DISK_ONLY, MEMORY_ONLY, etc. |
| **Cached Partitions** | How many partitions are actually cached |
| **Fraction Cached** | Percentage of total partitions that fit in cache |
| **Size in Memory** | Memory used by cached data |
| **Size on Disk** | Disk used by cached data (for DISK or spilled data) |

**When engineers use it**:
- Verify that `.cache()` actually materialised (requires an action first).
- Check if data spilled from memory to disk (Fraction Cached < 100%).
- After `.unpersist()`, verify the entry disappeared.

**Common mistakes**:
- Calling `.cache()` but never calling an action — the cache is lazy and won't populate until materialised.
- Caching a DataFrame that's only used once — adds overhead without benefit.

### Executors Tab

**What it shows**: Resource usage per executor (in local mode, you have one "driver" executor).

| Column | Meaning |
|--------|---------|
| **Executor ID** | "driver" or numeric IDs for cluster executors |
| **Address** | Host and port |
| **RDD Blocks** | Number of cached partitions stored on this executor |
| **Storage Memory** | Memory used for caching |
| **Disk Used** | Disk used for caching or spill |
| **Active Tasks** | Currently running tasks |
| **Failed Tasks** | Tasks that failed on this executor |
| **Complete Tasks** | Tasks successfully completed |
| **Total Tasks** | Sum of all tasks ever assigned to this executor |
| **Task Time (GC Time)** | Total CPU time and garbage collection time |
| **Input / Shuffle Read / Shuffle Write** | Aggregate I/O for this executor |

**When engineers use it**:
- Detect executor imbalance (one executor doing much more work than others).
- Monitor GC time — if GC is > 10% of task time, partitions may be too large or memory too low.
- Identify failed executors (OOM, lost heartbeat).

**Common mistakes**:
- In `local[*]` mode, everything runs on the "driver" — executor-level analysis is less meaningful.
- Ignoring GC time — it's often the hidden cause of slow stages.

### Environment Tab

**What it shows**: All active Spark configurations, system properties, JVM arguments, and classpath.

**When engineers use it**:
- Verify that configuration changes took effect (e.g., `spark.sql.adaptive.enabled`).
- Check `spark.sql.shuffle.partitions` value.
- Verify `spark.sql.autoBroadcastJoinThreshold`.
- Inspect JVM memory settings.

**Common mistakes**:
- Setting a config *after* `SparkSession.getOrCreate()` — it won't take effect because the session already exists.
- Thinking `spark.sql.shuffle.partitions = 200` in Environment means 200 tasks — AQE may coalesce at runtime.

---

## Section 3 — How to Read the DAG

The SQL/DataFrame tab shows a visual DAG (Directed Acyclic Graph). Reading it is a critical skill. The DAG flows **top to bottom**: data sources at the top, final output at the bottom.

### Node Reference

#### Scan (FileScan parquet / csv / orc)
Reads data from disk. Key metrics inside this node:
- `number of files read`: total files opened.
- `number of output rows`: rows produced after any pushed-down filters.
- `number of partitions read`: how many Hive-style partitions were read (partition pruning check).
- `size of files read total`: bytes read from disk.
- `PushedFilters`: filters applied at the Parquet reader level (predicate pushdown).
- `PartitionFilters`: filters applied on partition columns (partition pruning).

**Performance insight**: If `partitions read` equals the total number of partitions even though you have a filter on the partition column, partition pruning is not working.

#### Filter
Applies row-level filtering. Shows `number of output rows` — compare with the input (Scan output) to see how selective the filter is. More selective = more data eliminated early = faster.

#### Project
Column selection and computed columns (`withColumn`, `select`). This node is lightweight — just picking or computing columns. It does not shuffle or repartition data.

#### Exchange (THIS IS A SHUFFLE)
The most important node for performance. An Exchange means Spark is redistributing data across partitions. Every Exchange is a stage boundary. Key metrics:
- `shuffle records written`: rows sent to shuffle files.
- `shuffle write time total`: time spent writing shuffle data.
- `data size total`: total bytes shuffled (the key metric for shuffle cost).
- `records read`: rows consumed by the next stage from this shuffle.
- `local bytes read total (min, med, max)`: data read per task — **the max vs. median ratio indicates skew**.

There are different Exchange types:
- **HashPartitioning**: redistribute by hash of join/group key (most common).
- **RangePartitioning**: redistribute by sorted ranges (for `orderBy`).
- **RoundRobinPartitioning**: distribute evenly regardless of key (for `repartition(N)` without columns).
- **SinglePartition**: collect everything to one partition (for `coalesce(1)` or global sort).

#### BroadcastExchange
A special Exchange where a small DataFrame is serialised and sent to all executors in one shot — not shuffled. Appears before a `BroadcastHashJoin`. Much cheaper than a regular Exchange because there is no per-key redistribution.

#### HashAggregate
Performs hash-based aggregation (`groupBy(...).agg(...)`). Usually appears **twice** in the plan:
1. **Partial HashAggregate**: runs per partition before the shuffle — computes local partial results.
2. **Final HashAggregate**: runs after the shuffle — merges partial results from all partitions.

This two-phase pattern reduces shuffle volume: instead of shuffling every raw row, Spark shuffles only the partial aggregates.

#### SortMergeJoin
The default join when both sides are large. Both sides must be:
1. Shuffled by the join key (Exchange).
2. Sorted by the join key (Sort).
3. Merged in sorted order.

Appears as: `Exchange → Sort → SortMergeJoin ← Sort ← Exchange`.

Cost: 2 shuffles + 2 sorts. Expensive but works on any data size.

#### BroadcastHashJoin
Used when one side is small. The small side is broadcast to all executors, and each executor does a local hash-map lookup. No shuffle on the large side.

Appears as: `Scan → BroadcastHashJoin ← BroadcastExchange ← Scan`.

Cost: 1 broadcast (small cost) + 0 shuffles on the large side. Much faster than SortMergeJoin for small dimensions.

#### ShuffleHashJoin
Rare. Both sides are shuffled, but instead of sorting, Spark builds a hash table from the smaller side. Faster than SortMergeJoin when one side is moderately small (but too large to broadcast). Spark rarely chooses this automatically; it requires `spark.sql.join.preferSortMergeJoin = false`.

#### Sort
Sorts data within each partition. Appears before SortMergeJoin and before `orderBy`. Key metric: `sort time total`. High sort time on large partitions indicates that the partition is too large or the sort key has high cardinality.

#### Window
Implements window functions (`row_number`, `rank`, `sum().over(...)`). Requires data to be partitioned and sorted by the window specification. Causes a shuffle (partitionBy) + sort (orderBy) — functionally similar to SortMergeJoin's prerequisite.

#### Union
Concatenates two DataFrames vertically. No shuffle — just appends RDD lineages. Lightweight.

#### Repartition (Exchange with RoundRobinPartitioning or HashPartitioning)
Explicitly redistributes data. `repartition(N)` uses RoundRobinPartitioning. `repartition(N, col("x"))` uses HashPartitioning on column x.

#### Coalesce
Reduces the number of partitions **without** a full shuffle. It merges adjacent partitions on the same executor. Cheaper than repartition but can cause uneven partition sizes.

#### Limit / CollectLimit
Takes the first N rows. `CollectLimit` sends results to the driver. Appears at the bottom of the DAG for `show()` or `take()`.

### Special Nodes

#### WholeStageCodegen(N)
A wrapper node that means Spark generated optimised Java bytecode for all operations inside it. Everything within one WholeStageCodegen block runs as a single, tight loop — no virtual function calls between operators. UDFs break this boundary (the UDF runs as a separate function call inside the generated code, but prevents cross-operator optimisation).

If you see multiple WholeStageCodegen blocks separated by an Exchange, that is normal — codegen cannot span shuffle boundaries. If you see codegen broken within a single stage, a UDF or incompatible operation is the cause.

#### ColumnarToRow / RowToColumnar
Conversion nodes between columnar and row-based formats. `ColumnarToRow` appears after Parquet scans (Parquet is columnar; Spark's internal engine uses rows). In Spark 3.x with columnar execution enabled, `RowToColumnar` appears when feeding data to columnar operators. These are typically low-cost but show up as distinct nodes.

---

## Section 4 — Exchange and Shuffle Internals

Shuffle is the most expensive operation in Spark and the primary target of performance tuning.

### What Happens During a Shuffle

```
 Stage N (Map Side)                          Stage N+1 (Reduce Side)
┌──────────────────┐                        ┌──────────────────┐
│ Task 0           │                        │ Task 0           │
│ Process partition│──shuffle write──┐      │ Read data for    │
│ Output to local  │                │      │ hash bucket 0    │
│ shuffle files    │                │      └──────────────────┘
└──────────────────┘                │
┌──────────────────┐                ├──→   ┌──────────────────┐
│ Task 1           │                │      │ Task 1           │
│ Process partition│──shuffle write──┤      │ Read data for    │
│ Output to local  │                │      │ hash bucket 1    │
│ shuffle files    │                │      └──────────────────┘
└──────────────────┘                │
┌──────────────────┐                │      ┌──────────────────┐
│ Task 2           │                │      │ Task 2           │
│ Process partition│──shuffle write──┘      │ Read data for    │
│ Output to local  │                       │ hash bucket 2    │
│ shuffle files    │                       └──────────────────┘
└──────────────────┘
```

1. **Shuffle Write** (Map Side): Each task in Stage N computes its output, hashes each row by the shuffle key (join key, group key, etc.), and writes rows into local shuffle files — one file per reduce partition. This is written to the executor's local disk.

2. **Shuffle Read** (Reduce Side): Each task in Stage N+1 reads its corresponding hash bucket from all map-side tasks. In a cluster, this involves network I/O. In `local[*]` mode, it is all local disk reads.

### Key Shuffle Metrics in Spark UI

| Metric | Where | Meaning |
|--------|-------|---------|
| **Shuffle Write (Size/Records)** | Stages tab, Exchange node | Total data written to shuffle files by this stage |
| **Shuffle Read (Size/Records)** | Stages tab, Exchange node | Total data read from shuffle files by the next stage |
| **local bytes read** | Exchange node in SQL tab | Data read from same-executor shuffle files |
| **remote bytes read** | Exchange node in SQL tab | Data read from other executors over network |
| **fetch wait time** | Exchange node in SQL tab | Time tasks spent waiting for shuffle data to arrive |
| **records read (min, med, max)** | Exchange node in SQL tab | Per-task record counts — **max >> med = skew** |
| **local bytes read (min, med, max)** | Exchange node in SQL tab | Per-task byte counts — **max >> med = skew** |

### Detecting Shuffle Bottlenecks

1. **Absolute size**: If Shuffle Write exceeds 1 GB for a stage, look for ways to reduce it — broadcast small tables, filter before joins, use partial aggregation.

2. **Skew ratio**: In the Exchange node, compare `max` to `median` for `local bytes read`. If `max / median > 3`, you have skew. If > 10, you have severe skew.

3. **Fetch wait time**: If non-zero, tasks are waiting for shuffle data from other executors (network bottleneck). In local mode, this is always 0.

4. **Shuffle spill**: If a task's data doesn't fit in its allocated memory, Spark spills it to disk. Check the "Spill (Memory)" and "Spill (Disk)" metrics in the task table within a stage. Spill dramatically slows tasks because of disk I/O.

5. **Partition count**: If your shuffle produces 200 partitions but only 10 have data, 190 tasks are wasted. AQE coalescing or reducing `spark.sql.shuffle.partitions` fixes this.

---

## Section 5 — Join Strategies

### BroadcastHashJoin (BHJ)

**When Spark chooses it**: When one side's estimated size is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). Or when you explicitly use `broadcast()`.

**How it works**: The small table is collected to the driver, then broadcast to every executor. Each executor builds a hash map from the broadcast table and probes it for each row of the large table.

**DAG shape**: `Scan(large) → BroadcastHashJoin ← BroadcastExchange ← Scan(small)`

**Cost**: Zero shuffle on the large side. One broadcast (small table serialised once, sent everywhere). Fastest join strategy for small-large table pairs.

**When to force it**: `df.join(broadcast(smallDf), ...)` or increase `spark.sql.autoBroadcastJoinThreshold`.

### SortMergeJoin (SMJ)

**When Spark chooses it**: Default for two large tables when neither side is small enough to broadcast.

**How it works**: Both sides are shuffled by the join key, sorted, then merged in sorted order (like merge step of merge sort).

**DAG shape**: `Scan(left) → Exchange → Sort → SortMergeJoin ← Sort ← Exchange ← Scan(right)`

**Cost**: 2 shuffles + 2 sorts. Expensive but handles any data size. Vulnerable to skew on the join key.

### ShuffleHashJoin (SHJ)

**When Spark chooses it**: Rarely — requires `spark.sql.join.preferSortMergeJoin = false`. Used when one side is moderately small (larger than broadcast threshold but small enough to build a hash table per partition).

**How it works**: Both sides are shuffled, then the smaller side is built into a per-partition hash table.

**Cost**: 2 shuffles, no sort. Faster than SMJ when it applies, but risks OOM if the hash table is too large.

### CartesianJoin (Nested Loop Join)

**When Spark chooses it**: For cross joins or joins without an equi-condition.

**How it works**: Every row of the left side is matched against every row of the right side.

**Cost**: O(N × M). Extremely expensive. Avoid unless the result set is intentionally a cross product.

### Detecting Bad Join Strategies

- **SortMergeJoin on a small table**: If one side is < 100 MB, it should be broadcast. Check the Scan node's `size of files read` — if small, add `broadcast()` hint or increase the threshold.
- **Two Exchange nodes before a join**: Means both sides are being shuffled. If one side is small, a broadcast would eliminate one Exchange.
- **BroadcastHashJoin with a large table**: If the broadcast table is > 1 GB, you'll see high driver memory usage and potential OOM. Reduce the threshold or avoid the broadcast hint.

---

## Section 6 — Stage-Level Debugging

Click into any stage in the Stages tab to access the detailed task-level view.

### Summary Metrics Table

This table shows percentile statistics across all tasks in the stage:

| Metric | Min | 25th | Median | 75th | Max |
|--------|-----|------|--------|------|-----|

Key rows to examine:

#### Duration
- **Healthy**: min and max are within 2–3× of each other.
- **Skewed**: max is 5–100× larger than median. One or a few tasks are processing much more data.

#### GC Time
- **Healthy**: < 5% of task duration.
- **Unhealthy**: > 10% of task duration. Partitions too large or executor memory too low. Fix by increasing partitions (smaller partition = less memory per task) or increasing `spark.executor.memory`.

#### Shuffle Read Size / Records
- **Healthy**: even distribution (min ≈ max within 3×).
- **Skewed**: max >> median. A hot key is concentrating data in one partition.

#### Spill (Memory) / Spill (Disk)
- **Healthy**: 0 / 0.
- **Unhealthy**: Any non-zero spill means a task ran out of execution memory and wrote intermediate data to disk. Fix by increasing `spark.executor.memory`, increasing partition count (less data per task), or resolving skew.

#### Peak Execution Memory
- High values indicate complex operations (large hash tables in joins/aggregations). If close to executor memory limits, risk of OOM.

### Detecting Data Skew

The definitive skew indicator is in the **task table** within a stage. Sort by "Duration" descending. If the top 1–3 tasks take 10×+ longer than the rest, you have skew. Cross-reference with "Shuffle Read Size" — the slow tasks will have read much more data.

**Formula**: `skew_factor = max_task_shuffle_read / median_task_shuffle_read`
- `< 3`: acceptable.
- `3–10`: moderate skew, consider optimising.
- `> 10`: severe skew, job is bottlenecked on one task.

### Detecting Stragglers

A straggler is a single slow task that holds up the entire stage. Causes:
- **Data skew**: one partition has more data (see above).
- **Executor issues**: one executor has high GC, disk I/O, or network problems.
- **Speculative execution**: if enabled, Spark launches a duplicate task. Check for "speculated" tasks in the task table.

### Detecting Executor Imbalance

In the Executors tab, compare "Total Tasks" and "Task Time" across executors. If one executor ran significantly more tasks or took more time, it may have more cached data, or the scheduler assigned more work to it. In local mode, this is not relevant (single executor).

---

## Section 7 — Adaptive Query Execution

AQE runs after each shuffle stage and can modify the remaining plan based on actual statistics.

### Feature 1: Coalesced Shuffle Partitions

**Problem**: `spark.sql.shuffle.partitions = 200` creates 200 post-shuffle partitions, but maybe only 20 contain meaningful data. 180 tasks process empty or tiny partitions — wasted scheduling overhead.

**AQE solution**: After the shuffle write completes, AQE examines actual partition sizes. It merges adjacent small partitions until each is at least `spark.sql.adaptive.coalescePartitions.minPartitionSize` (default 1 MB). Result: 200 partitions → maybe 15.

**Spark UI indicator**: In the SQL tab, you see `AQEShuffleRead` or `CustomShuffleReader` instead of a regular Exchange read. The actual number of tasks in the next stage is less than `spark.sql.shuffle.partitions`.

**Key configs**:
- `spark.sql.adaptive.coalescePartitions.enabled = true`
- `spark.sql.adaptive.coalescePartitions.minPartitionSize = 1MB`
- `spark.sql.adaptive.coalescePartitions.initialPartitionNum = 200` (starting point)

### Feature 2: Dynamic Join Strategy Switch

**Problem**: At plan time, Spark estimates table sizes and picks SortMergeJoin because the estimate is above the broadcast threshold. But after filtering, the actual size is much smaller.

**AQE solution**: After the shuffle, AQE checks actual data size. If one side is below the broadcast threshold, it switches to BroadcastHashJoin — eliminating the shuffle on the other side.

**Spark UI indicator**: The SQL tab plan shows `SortMergeJoin` initially, but if you re-check after execution (with AQE active), it may show `BroadcastHashJoin`. The DAG has `AdaptiveSparkPlan` as the root, indicating runtime replanning.

### Feature 3: Skew Join Optimization

**Problem**: A shuffle produces partitions where one is 10× larger than the median. The join stage is bottlenecked on that one straggler task.

**AQE solution**: AQE detects the skewed partition (based on `skewedPartitionFactor` and `skewedPartitionThresholdInBytes`), splits it into N smaller sub-partitions, and replicates the corresponding partition from the other join side. The join then runs on smaller, balanced chunks.

**Spark UI indicator**: More tasks than expected in the join stage. The SQL tab may show `SkewJoin` annotations. The task durations become more uniform.

**Key configs**:
- `spark.sql.adaptive.skewJoin.enabled = true`
- `spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5` (partition is skewed if > 5× median)
- `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB`

---

## Section 8 — Performance Debugging Playbook

### Slow Join

1. **SQL tab**: Check join strategy. Is it SortMergeJoin on a small table? → Add `broadcast()`.
2. **Exchange nodes**: Two full shuffles before the join? → Broadcast eliminates one.
3. **Stage durations**: Which side's shuffle is slower? → Filter or sample the larger side.
4. **Task skew in join stage**: max >> median? → Salt the join key or enable AQE skew join.

### Skewed Key

1. **Stages tab**: Find the slow stage. Click into it.
2. **Task table**: Sort by Duration descending. Check if top task's Shuffle Read is much larger.
3. **Fix options**: Salt the key (Experiment 01), enable AQE skew join (Experiment 05), or isolate hot keys into a separate broadcast join.

### Too Many Shuffle Partitions

1. **Stages tab**: A stage has 200 tasks but most finish in < 100ms → overhead-dominated.
2. **SQL tab Exchange**: `data size total` is small (< 50 MB) but split across 200 partitions.
3. **Fix**: Reduce `spark.sql.shuffle.partitions` or enable AQE coalescing.

### Missing Broadcast Join

1. **SQL tab**: Two Exchange nodes before a join — both sides being shuffled.
2. **Check small side**: Scan node → `size of files read`. If < 100 MB, it should be broadcast.
3. **Fix**: `broadcast(smallDf)` or increase `spark.sql.autoBroadcastJoinThreshold`.

### Partition Pruning Not Working

1. **SQL tab → Scan node**: Check `PartitionFilters`. If empty or `[]`, pruning did not activate.
2. **`number of partitions read`**: Equals total partitions → everything was read.
3. **Cause**: Your filter is not on the partition column, or you used a UDF/cast that prevents pushdown.
4. **Fix**: Filter directly on the partition column with literal values. Avoid wrapping it in functions.

### Slow Parquet Scan

1. **SQL tab → Scan node**: `number of files read` is very high → small file problem.
2. **scan time total**: Compare min/max — if max is very high, some files are large or on slow storage.
3. **Fix**: Compact small files by repartitioning and rewriting. Use `repartition(N, col("partition_col"))`.

### Executor GC Pressure

1. **Executors tab**: GC Time is > 10% of Task Time.
2. **Stages tab**: Individual task GC times are high (visible in task detail).
3. **Cause**: Partitions too large, or executor memory too small.
4. **Fix**: Increase `spark.sql.shuffle.partitions` (smaller partitions), increase `spark.executor.memory`, or reduce cached data.

---

## Section 9 — Optimization Cheat Sheet

### Detect Skew Quickly
```
In Stages tab → click the slow stage → Summary Metrics:
  Shuffle Read Size: min = 1 MB, median = 10 MB, max = 500 MB
  → max / median = 50 → SEVERE SKEW
```

### Estimate Optimal Shuffle Partitions
```
optimal = total_shuffle_data_size / target_partition_size

Example:
  Shuffle Write in Exchange node = 2 GB
  Target = 128 MB per partition
  Optimal = 2048 / 128 = 16 partitions

  Set: spark.sql.shuffle.partitions = 16
  Or: enable AQE coalescing (automatic)
```

### Detect Unnecessary Shuffles
```
In SQL tab, count Exchange nodes.
  0 Exchanges: no shuffle (ideal for simple transforms)
  1 Exchange: one shuffle (groupBy or repartition)
  2 Exchanges: typical for SortMergeJoin (one per side)
  3+ Exchanges: look for unnecessary repartitions or multiple joins

Can any Exchange be replaced by BroadcastExchange?
  → Check the Scan node sizes on both sides.
```

### Detect Inefficient Joins
```
SQL tab → find the join node.
  SortMergeJoin + small table? → use broadcast()
  Two Exchanges before a join? → one should be BroadcastExchange
  Task skew in join stage? → salt or AQE skew join
```

### Detect Bad Partitioning
```
Stages tab:
  200 tasks, most complete in < 50ms? → too many partitions
  5 tasks, each takes 5 minutes? → too few partitions
  1 task 10x slower than rest? → skew

Exchange node:
  data size total = 50 MB across 200 partitions? → each partition = 250 KB (too small)
  data size total = 10 GB across 10 partitions? → each partition = 1 GB (too large)
```

### Detect Missing Predicate Pushdown
```
SQL tab → Scan node:
  PushedFilters: [] → no predicate pushdown
  PartitionFilters: [] → no partition pruning

  Should see something like:
  PushedFilters: [IsNotNull(amount), GreaterThan(amount, 1000)]
  PartitionFilters: [isnotnull(event_date), event_date >= 2024-01-01]
```

---

## Section 10 — How Senior Engineers Use Spark UI

### The Real-World Workflow

When a senior Spark engineer debugs a slow production job, they follow a consistent workflow. It is not random exploration — it is a systematic funnel from broad to narrow.

**Step 1 — Open Jobs tab, find the slow job.**
Note the duration and stage/task counts. Is it one job or many? If many jobs do similar work, suspect missing caching.

**Step 2 — Open SQL tab, inspect the physical plan.**
This is the most information-dense view. Count Exchange nodes (shuffles). Identify join strategies. Check Scan nodes for partition pruning and file counts. Note row counts at each step — where does data explode or shrink?

**Step 3 — Identify the most expensive Exchange.**
Look at `data size total` inside each Exchange. The largest one is the primary optimisation target. Can the table on the other side of this Exchange be broadcast?

**Step 4 — Open Stages tab, find the slowest stage.**
Click into it. Check Summary Metrics. Compare min/median/max task durations and shuffle read sizes. If max >> median, you have skew.

**Step 5 — Check task-level details for the slow stage.**
Sort by Duration descending. Is one task 10× slower? Check its Shuffle Read Size, GC Time, and Spill. This pinpoints the root cause: skew, GC, or spill.

**Step 6 — Check Executors tab.**
Is GC Time high? Is one executor doing disproportionate work? Are there failed tasks?

**Step 7 — Formulate the fix.**
Based on findings:
- Skew → salt, AQE skew join, or isolate hot keys.
- Large shuffle → broadcast small table or filter earlier.
- Too many partitions → reduce shuffle.partitions or enable AQE.
- GC pressure → increase memory or reduce partition size.
- Missing pushdown → rewrite filters on partition columns.

**Step 8 — Apply fix, re-run, compare.**
Re-run the job with the fix. Compare the same metrics: Exchange sizes, stage durations, task skew. The Spark UI makes before/after comparison straightforward — the structure is the same, only the numbers change.

### What Sets Experts Apart

Experts do not look at Spark UI linearly. They jump directly to the SQL tab, scan for red flags (unexpected SortMergeJoin, missing BroadcastExchange, high Exchange sizes), form a hypothesis, then validate with Stages and Executors. The entire process takes 2–5 minutes for a familiar workload. For an unfamiliar workload, they start from the DAG shape and work outward.

The single most valuable skill is **counting Exchanges and reading their sizes**. If you can do this fluently, you can debug 80% of Spark performance issues.

