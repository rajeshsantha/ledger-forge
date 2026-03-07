# Spark UI Experiment Playbook

> A hands-on guide for using Spark UI to observe, measure, and validate
> performance optimizations during controlled experiments.

---

## Table of Contents

1. [How to Observe Spark UI While Running Experiments](#section-1--how-to-observe-spark-ui-while-running-experiments)
2. [What to Look For During Each Experiment Type](#section-2--what-to-look-for-during-each-experiment-type)
3. [Spark UI Investigation Workflow](#section-3--spark-ui-investigation-workflow)
4. [Spark UI Signals That Indicate Problems](#section-4--spark-ui-signals-that-indicate-problems)
5. [Deep Learning Exercise](#section-5--deep-learning-exercise)

---

## Section 1 — How to Observe Spark UI While Running Experiments

When you run a Spark experiment, Spark UI at `http://localhost:4040` is your real-time dashboard. Each tab serves a different purpose during observation. Here is what to track in each tab and when.

### Jobs Tab — Track During Execution

While the experiment is running, refresh the Jobs tab periodically.

| What to Watch | Why |
|---------------|-----|
| **Active Jobs count** | Confirms your action triggered execution. If you see 0 active jobs, the action already completed or was never called. |
| **Stages: Succeeded/Total** | Tells you the progress of the current job. A job with 4 stages means 3 shuffle boundaries. If it stalls at 2/4, the third stage is the bottleneck. |
| **Tasks: Succeeded/Total** | Shows task-level progress. If it reads `180/423 (9 running)`, only 9 cores are actively processing tasks. The rest are waiting. |
| **Duration** | Compare across experiment runs. The baseline job duration vs. optimized job duration is your primary success metric. |
| **Job count** | If you see more jobs than expected, your code is triggering unintended actions. Each `count()`, `show()`, `collect()`, `explain()` with action, or `write` creates a separate job. |

**Tip**: After both baseline and optimized runs complete, note the job durations in a table for comparison. Do not rely on memory.

### Stages Tab — Track After Each Stage Completes

Switch to the Stages tab as soon as a stage finishes. Completed stages show full metrics.

| What to Watch | Why |
|---------------|-----|
| **Duration per stage** | Identify the bottleneck stage. The slowest stage dominates wall-clock time. |
| **Shuffle Read / Shuffle Write** | Quantifies how much data crosses stage boundaries. Compare these values between baseline and optimized runs to see if you reduced shuffle volume. |
| **Task count** | With AQE, the actual task count may differ from `spark.sql.shuffle.partitions`. Note the actual count — it tells you how AQE coalesced partitions. |
| **Input column** | Shows data read from disk. Only appears in scan stages. Compare with the `number of files read` in the SQL tab to verify partition pruning. |

**Tip**: Click into the completed stage to access the Summary Metrics table. This is where you detect skew (compare min/median/max task duration and shuffle read size).

### SQL / DataFrame Tab — Inspect After Job Completes

The SQL tab is most useful after the job finishes, because all runtime metrics are populated.

| What to Watch | Why |
|---------------|-----|
| **DAG shape** | Count Exchange nodes (shuffles). Compare the DAG shape between baseline and optimized — you should see fewer Exchanges or different join nodes after optimization. |
| **Row counts at each node** | Trace how data flows. Where does the row count drop (filters)? Where does it stay flat (projections)? Where does it explode (cross joins, explodes)? |
| **Exchange metrics (min, med, max)** | The max-to-median ratio in `local bytes read` reveals skew. Track this across runs. |
| **Join node type** | Confirm the join strategy changed (e.g., SortMergeJoin in baseline, BroadcastHashJoin in optimized). |
| **Scan node details** | Check `PartitionFilters`, `PushedFilters`, `number of partitions read`, and `number of files read`. These confirm whether predicate pushdown and partition pruning are active. |

**Tip**: Always enable the checkbox "Show the Stage ID and Task ID that corresponds to the max metric." This lets you trace any outlier metric back to the exact task that caused it.

### Storage Tab — Check During Caching Experiments

| What to Watch | Why |
|---------------|-----|
| **Entry appears** | Confirms `.cache()` or `.persist()` materialised. If no entry, you forgot to trigger an action after caching. |
| **Size in Memory vs. Size on Disk** | Shows how much memory the cache consumes. If "Size on Disk" is non-zero for a MEMORY_AND_DISK cache, memory was insufficient and data spilled to disk. |
| **Fraction Cached** | Should be 100%. If less, some partitions were evicted. |
| **Entry disappears after unpersist** | Confirms cleanup. Leaked caches waste memory for subsequent stages. |

### Executors Tab — Check for Resource Issues

| What to Watch | Why |
|---------------|-----|
| **GC Time vs. Task Time** | If GC exceeds 10% of task time, partitions are too large or memory is too low. |
| **Failed Tasks** | Any non-zero value indicates OOM or other executor failures. |
| **Shuffle Read / Write totals** | Aggregate view of I/O per executor. Useful in cluster mode to detect executor imbalance. |

---

## Section 2 — What to Look For During Each Experiment Type

### Broadcast Join Experiment

**Goal**: Replace SortMergeJoin with BroadcastHashJoin for a small dimension table.

| Observation Point | Baseline (No Broadcast) | Optimized (Broadcast) |
|-------------------|------------------------|-----------------------|
| **SQL tab — join node** | `SortMergeJoin` | `BroadcastHashJoin` |
| **SQL tab — Exchange count** | 2 Exchanges (one per join side) | 1 BroadcastExchange (small side only) + 0 Exchange on large side |
| **Stages tab — stage count** | More stages (shuffle + sort on both sides) | Fewer stages (no shuffle on large side) |
| **Stages tab — shuffle write** | Large shuffle write for both sides | No shuffle write for the large side |
| **SQL tab — Scan node** | Both sides scanned normally | Small side shows `BroadcastExchange` wrapper |
| **Duration** | Slower (shuffle + sort overhead) | Faster (local hash lookup, no network shuffle) |

**Key confirmation**: The SQL tab DAG should show `BroadcastExchange` feeding into `BroadcastHashJoin` instead of two regular `Exchange` nodes feeding into `SortMergeJoin`.

### Skewed Join Experiment

**Goal**: Redistribute hot keys to eliminate straggler tasks.

| Observation Point | Baseline (Naive Join) | Optimized (Salted Join or AQE Skew) |
|-------------------|-----------------------|--------------------------------------|
| **Stage detail — task duration** | max >> median (e.g., max = 60s, median = 5s) | max close to median (e.g., max = 8s, median = 5s) |
| **Exchange node — local bytes read (max)** | One partition holds disproportionate data | Data distributed more evenly |
| **Stage detail — Spill** | Straggler task may spill to disk | No spill (smaller partitions) |
| **Overall job duration** | Dominated by the slowest task | Faster (bottleneck eliminated) |
| **SQL tab — Exchange** | Standard HashPartitioning | Salted: HashPartitioning on (key, salt). AQE: may show `SkewJoin` |

**Key confirmation**: Click into the join stage for both runs. Sort the task table by Duration descending. In the baseline, the top task is 10x+ slower. In the optimized run, task durations are uniform.

### Partition Pruning Experiment

**Goal**: Reduce files read by filtering on the partition column.

| Observation Point | Baseline (Full Scan) | Optimized (Filter on Partition Column) |
|-------------------|----------------------|----------------------------------------|
| **SQL tab — Scan node — `number of partitions read`** | All partitions (e.g., 30) | Only matching partitions (e.g., 3) |
| **SQL tab — Scan node — `number of files read`** | All files (e.g., 240) | Subset of files (e.g., 24) |
| **SQL tab — Scan node — `PartitionFilters`** | Empty `[]` | Contains the filter expression |
| **SQL tab — Scan node — `size of files read`** | Full dataset size | Fraction of dataset size |
| **Stages tab — Input** | Large (full dataset) | Small (pruned subset) |
| **Duration** | Proportional to full dataset | Proportional to filtered subset |

**Key confirmation**: The `PartitionFilters` field in the Scan node must contain your filter expression. If it is empty, Spark did not push the filter down to the file scan level.

### Caching Experiment

**Goal**: Avoid recomputing an expensive DataFrame used by multiple actions.

| Observation Point | Baseline (No Cache) | Optimized (Cached) |
|-------------------|--------------------|--------------------|
| **Jobs tab — job count** | 3 jobs, each recomputing the full DAG | 3 jobs, but jobs 2 and 3 skip early stages |
| **Stages tab — skipped stages** | None | Stages before the cache point show "Skipped" |
| **Storage tab** | Empty | Shows the cached DataFrame with size info |
| **Total wall-clock time** | 3x the single-run time | ~1x + small overhead for cache hits |

**Key confirmation**: In the Stages tab, look for stages marked as "Skipped" in jobs 2 and 3. Skipped means Spark found the data already materialised in cache and did not recompute it. Also verify the Storage tab shows your cached DataFrame.

### Adaptive Query Execution Experiment

**Goal**: Let AQE dynamically optimize partitions, joins, and skew at runtime.

| Observation Point | Baseline (AQE OFF) | Optimized (AQE ON) |
|-------------------|--------------------|--------------------|
| **SQL tab — root node** | Regular plan root | `AdaptiveSparkPlan isFinalPlan=true` |
| **Stages tab — task count in shuffle stages** | Exactly `spark.sql.shuffle.partitions` (e.g., 200) | Fewer tasks (e.g., 7-20, coalesced by AQE) |
| **SQL tab — join node** | Always SortMergeJoin (if both sides large) | May switch to BroadcastHashJoin if one side is small after filtering |
| **Stage detail — task duration distribution** | Skewed if data is skewed | More uniform if AQE skew join is enabled |
| **SQL tab — Exchange read** | Regular shuffle read | May show `AQEShuffleRead` or `CustomShuffleReader` |

**Key confirmation**: Compare task counts in shuffle stages. AQE OFF should show exactly 200 (or whatever `shuffle.partitions` is set to). AQE ON should show a much smaller number, confirming partition coalescing.

### Shuffle Partition Tuning Experiment

**Goal**: Find the optimal `spark.sql.shuffle.partitions` value.

| Observation Point | Too Few (e.g., 10) | Too Many (e.g., 1000) | Optimal (e.g., 50) |
|-------------------|--------------------|-----------------------|---------------------|
| **Stages tab — task count** | 10 tasks | 1000 tasks | 50 tasks |
| **Stage detail — task duration** | Each task slow (processing ~5GB each) | Each task fast (< 100ms, mostly overhead) | Balanced (1-5s each) |
| **Stage detail — GC time** | High (large partitions pressure GC) | Low | Low |
| **Stage detail — spill** | Possible (partitions too large) | None | None |
| **Overall duration** | Slow (few fat tasks) | Slow (scheduling overhead for 1000 tiny tasks) | Fastest |

**Key confirmation**: Plot duration vs. partition count. The optimal point minimises total duration. AQE auto-coalescing should arrive near this optimal point automatically.

### UDF vs Native Expression Experiment

**Goal**: Replace a Scala UDF with native `when/otherwise` to enable WholeStageCodegen.

| Observation Point | Baseline (UDF) | Optimized (Native) |
|-------------------|----------------|---------------------|
| **SQL tab — WholeStageCodegen** | Codegen boundary broken at UDF (multiple codegen blocks within one stage) | Single WholeStageCodegen block wrapping the entire stage |
| **SQL tab — plan nodes** | Shows UDF invocation node | Shows `CaseWhen` or `If` expression inline |
| **Stage detail — task duration** | Slower per task (serialization overhead) | Faster per task (vectorized execution) |
| **Overall duration** | 2-10x slower | Baseline for comparison |

**Key confirmation**: In the SQL tab, look for `WholeStageCodegen(N)` wrapping. With UDFs, you will see the codegen block split — the UDF appears as a separate node that breaks the pipeline. With native expressions, the entire scan-filter-project-aggregate chain is inside one codegen block.

---

## Section 3 — Spark UI Investigation Workflow

Follow this seven-step workflow every time you run an experiment. It takes 3-5 minutes per run and builds a consistent mental model.

### Step 1 — Open SQL Tab and Inspect the Physical Plan

Navigate to `SQL / DataFrame` tab. Click the query entry. This shows the visual DAG with runtime metrics.

**Actions**:
- Read the DAG from top (sources) to bottom (output).
- Note the shape: how many branches? Where do they converge (joins)?
- Identify all `Exchange` nodes — each is a shuffle.
- Identify all join nodes — note the join type (SortMergeJoin, BroadcastHashJoin, etc.).

### Step 2 — Count Exchanges

Count the total number of `Exchange` boxes in the DAG.

**Interpretation**:
- 0 Exchanges: no shuffle at all (simple scan + filter + project). Ideal.
- 1 Exchange: one shuffle (a single groupBy, or a broadcast join with one exchange on the small side).
- 2 Exchanges: typical for SortMergeJoin or a groupBy after a join.
- 3+ Exchanges: multiple joins, window functions, or unnecessary repartitions.

**Question to ask**: Can any `Exchange` be replaced with a `BroadcastExchange`? Check the Scan node on each side of a join — if one side's `size of files read` is under 100 MB, it should be broadcast.

### Step 3 — Identify Joins

Find all join nodes in the DAG. For each join:
- Note the join type: `SortMergeJoin`, `BroadcastHashJoin`, `ShuffleHashJoin`.
- Check if the correct strategy is being used (small table should be broadcast).
- Note the join key (visible in the node details or `explain()` output).

### Step 4 — Analyze Shuffle Stages

For each `Exchange` node, read the metrics inside the box:
- `shuffle records written`: total rows entering the shuffle.
- `data size total (min, med, max)`: total bytes and per-task distribution.
- `local bytes read total (min, med, max)`: how evenly data is distributed across reduce tasks.

**Compute the skew ratio**: `max / median` from `local bytes read`. If > 3, you have a skew problem.

### Step 5 — Check Task Skew

Switch to the Stages tab. Click into the slowest stage. Look at the Summary Metrics table:

| Metric | Check |
|--------|-------|
| Duration (min, median, max) | Is max > 3x median? → Skew or straggler |
| Shuffle Read Size (min, median, max) | Is max > 3x median? → Data skew on the shuffle key |
| GC Time | Is max GC > 10% of max duration? → Memory pressure |
| Spill (Disk) | Any non-zero value? → Partition too large for memory |

### Step 6 — Inspect Executors

Open the Executors tab. In cluster mode, compare across executors:
- Is one executor's `Task Time` much higher? → Possible data locality issue or hot executor.
- Is `GC Time` high for any executor? → That executor's tasks have large partitions.
- Any `Failed Tasks`? → OOM or heartbeat timeout.

In `local[*]` mode, just check the driver's GC Time and Task Time ratio.

### Step 7 — Confirm Optimization

After running the optimized version, revisit Steps 1-6 and compare:

| Metric | Baseline | Optimized | Improved? |
|--------|----------|-----------|-----------|
| Exchange count | ? | ? | Fewer = better |
| Join strategy | ? | ? | BHJ = better than SMJ for small tables |
| Shuffle data size | ? | ? | Smaller = better |
| Skew ratio (max/median) | ? | ? | Closer to 1 = better |
| Slowest stage duration | ? | ? | Shorter = better |
| Total job duration | ? | ? | The bottom line |

Fill in this table for every experiment. It becomes your evidence that the optimization worked.

---

## Section 4 — Spark UI Signals That Indicate Problems

### Signal: Huge Shuffle Write (> 1 GB)

**Where you see it**: Stages tab (Shuffle Write column) or Exchange node in SQL tab.

**Root cause**: Too much data crossing a stage boundary. Common when joining two large tables or doing a groupBy on high-cardinality keys without prior filtering.

**Fix**:
- Filter data before the join (reduce rows entering the shuffle).
- Broadcast the small side of a join (eliminates one shuffle entirely).
- Use partial aggregation (groupBy + agg already does this automatically).
- Project only needed columns before the shuffle (reduces per-row byte size).

### Signal: One Task Running Much Longer Than Others

**Where you see it**: Stage detail page. Sort task table by Duration descending. Top task is 10x+ slower.

**Root cause**: Data skew. One partition has far more data than others because the shuffle key has a hot value.

**Fix**:
- Salt the skewed key (add random suffix, replicate the small side).
- Enable AQE skew join (`spark.sql.adaptive.skewJoin.enabled = true`).
- Isolate the hot key: process it separately with a broadcast join, then union the results.

### Signal: High GC Time (> 10% of Task Time)

**Where you see it**: Executors tab (GC Time column) or Stage detail (GC Time in Summary Metrics).

**Root cause**: Partitions are too large, causing the JVM to spend excessive time garbage collecting. Or executor memory is insufficient for the workload.

**Fix**:
- Increase `spark.sql.shuffle.partitions` (more partitions = smaller per-task data).
- Increase `spark.executor.memory`.
- Reduce cached data if the Storage tab shows high memory usage.
- Switch from MEMORY_AND_DISK to DISK_ONLY for large caches.

### Signal: Too Many Tiny Tasks (200 tasks, each < 100ms)

**Where you see it**: Stage detail page. Most tasks complete in under 100ms. Total task time is small, but the stage duration is disproportionately long due to scheduling overhead.

**Root cause**: `spark.sql.shuffle.partitions` is too high for the actual data volume. Each partition has very little data, but Spark still pays scheduling and serialization costs per task.

**Fix**:
- Reduce `spark.sql.shuffle.partitions` (e.g., from 200 to 20).
- Enable AQE coalescing (`spark.sql.adaptive.coalescePartitions.enabled = true`) to let Spark merge tiny partitions automatically.

### Signal: Missing Broadcast Join (SortMergeJoin on a Small Table)

**Where you see it**: SQL tab. Two `Exchange` nodes feed into a `SortMergeJoin`. One side's Scan node shows a small `size of files read` (e.g., 5 MB).

**Root cause**: Spark estimated the small table as larger than `spark.sql.autoBroadcastJoinThreshold` (default 10 MB), possibly because statistics are stale or unavailable.

**Fix**:
- Use `broadcast(smallDf)` hint explicitly.
- Increase `spark.sql.autoBroadcastJoinThreshold` (e.g., to 100 MB).
- Run `ANALYZE TABLE` to update statistics if using Hive tables.
- With AQE enabled, Spark may auto-switch after seeing actual sizes.

### Signal: Missing Partition Pruning

**Where you see it**: SQL tab, Scan node. `PartitionFilters: []` is empty even though you have a filter on what you think is the partition column. `number of partitions read` equals total partitions.

**Root cause**:
- Your filter is on a non-partition column (e.g., filtering on `event_ts` instead of `event_date`).
- Your filter uses a function or cast that prevents pushdown (e.g., `year(event_date) = 2024` instead of `event_date >= '2024-01-01'`).
- The data is not actually partitioned (Parquet files are in a flat directory, not Hive-style `col=value/` directories).

**Fix**:
- Filter directly on the partition column using simple comparisons with literals.
- Avoid wrapping partition columns in functions.
- Verify the data is written with `.partitionBy("partition_column")`.

### Signal: Codegen Disabled (No WholeStageCodegen in SQL Tab)

**Where you see it**: SQL tab. Operations appear as individual nodes without a `WholeStageCodegen(N)` wrapper. Or codegen blocks are fragmented within a single stage.

**Root cause**: A UDF, a complex data type, or an unsupported operation broke the codegen pipeline. Spark falls back to interpreted (row-at-a-time) execution.

**Fix**:
- Replace UDFs with native Spark expressions (`when/otherwise`, built-in functions).
- If a UDF is unavoidable, minimize its scope — apply it late in the pipeline after filters and projections have reduced the data volume.
- Check for unsupported types (e.g., deeply nested structs, maps with complex keys).

---

## Section 5 — Deep Learning Exercise

Follow this checklist every time you run an experiment. The goal is not just to see numbers, but to build intuition for how code changes map to physical plan changes, which map to runtime behavior.

### Before Running

- [ ] Write down your hypothesis: "I expect this change to reduce shuffle volume by X%" or "I expect the join stage to use BroadcastHashJoin instead of SortMergeJoin."
- [ ] Note the key configs: `shuffle.partitions`, `autoBroadcastJoinThreshold`, `adaptive.enabled`.
- [ ] Open Spark UI in a browser tab, ready to observe.

### During Baseline Run

- [ ] Note the total job duration from the Jobs tab.
- [ ] Count Exchange nodes in the SQL tab DAG.
- [ ] For each Exchange, record `data size total` and `records written`.
- [ ] Identify the join strategy in the SQL tab (SMJ, BHJ, SHJ).
- [ ] Click into the slowest stage. Record the Summary Metrics: min/median/max for Duration, Shuffle Read Size, GC Time, Spill.
- [ ] Compute skew ratio: `max shuffle read / median shuffle read`.
- [ ] Check the Storage tab: anything cached?
- [ ] Check Executors tab: note GC Time percentage.

### During Optimized Run

- [ ] Repeat every observation from the baseline checklist above.
- [ ] Specifically compare: Exchange count, join strategy, shuffle data size, skew ratio, stage durations, total job duration.

### After Both Runs

- [ ] Fill in the comparison table:

| Metric | Baseline | Optimized | Delta |
|--------|----------|-----------|-------|
| Total duration | | | |
| Exchange count | | | |
| Join strategy | | | |
| Largest shuffle (data size) | | | |
| Skew ratio (max/med) | | | |
| Slowest stage duration | | | |
| GC time (% of task time) | | | |
| Spill to disk | | | |
| Task count in shuffle stage | | | |

- [ ] Validate your hypothesis: did the expected change happen? If not, why?
- [ ] Write a one-sentence summary: "Broadcast join eliminated the 547 MiB shuffle on the transaction side and reduced total time from 41s to 12s."

### Building Long-Term Intuition

After 5-10 experiments following this checklist, you will develop the ability to:

1. **Predict the DAG from code**: Before running, mentally sketch the DAG. How many Exchanges? What join strategy? Then verify with Spark UI.
2. **Estimate shuffle volume**: Knowing the row count and average row size, estimate the Exchange data size before running. Then verify.
3. **Predict where skew will appear**: If you know the key distribution (Zipf, uniform), predict which stage will have skewed tasks. Then verify.
4. **Read a DAG in under 30 seconds**: Scan for Exchange count, join types, and Scan node metrics. Form a hypothesis immediately. This is the skill that senior engineers use daily.

The Spark UI is not a passive monitoring tool. It is a laboratory instrument. Every experiment you run is a chance to calibrate your understanding of Spark internals. Use it deliberately, not casually.

