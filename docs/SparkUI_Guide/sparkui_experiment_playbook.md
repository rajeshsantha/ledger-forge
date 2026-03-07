# Spark UI — Experiment Observation Playbook

> **Purpose:** A hands-on guide for deeply observing the Spark UI while running the 8 LedgerForge performance experiments.  
> **Assumption:** You have the Spark UI running at `http://localhost:4040` and are executing experiments from IntelliJ or terminal.  
> **Goal:** After each experiment, you should be able to articulate *exactly* why the optimized version is faster, in terms of plan nodes, exchange counts, task distributions, and specific metrics.

---

## Table of Contents

1. [How to Observe Spark UI While Running Experiments](#section-1--how-to-observe-spark-ui-while-running-experiments)
2. [What to Look For During Each Experiment Type](#section-2--what-to-look-for-during-each-experiment-type)
3. [Spark UI Investigation Workflow](#section-3--spark-ui-investigation-workflow)
4. [Spark UI Signals That Indicate Problems](#section-4--spark-ui-signals-that-indicate-problems)
5. [Deep Learning Exercise Checklist](#section-5--deep-learning-exercise-checklist)

---

## Section 1 — How to Observe Spark UI While Running Experiments

The key insight: **open Spark UI before the job starts, not after it finishes**. Most experiments complete in minutes — if you only look after completion, you miss the live execution view. Open four browser tabs pre-loaded with the four most useful views.

### Browser Setup (Before Running Any Experiment)

```
Tab 1: http://localhost:4040/jobs/        ← watch active jobs
Tab 2: http://localhost:4040/stages/      ← watch active stages and task distributions
Tab 3: http://localhost:4040/SQL/         ← inspect physical plans
Tab 4: http://localhost:4040/executors/   ← watch resource usage
```

Refresh manually every 10–30 seconds during execution. Spark UI does not auto-refresh.

---

### Jobs Tab — What to Track Live

While a job is running, the Jobs tab tells you:

- **How many stages are total vs. how many have completed.** `2/4` means 4 stages exist, 2 are done, 2 are in progress or pending. Count the stages before and after optimization to confirm you've reduced shuffles.
- **Task progress bar.** The blue bar filling up shows task completion. If the bar stalls at 90% for a long time, one or a few tasks are stragglers — likely skew.
- **Job description.** The description links to the source file and line number. Use this to confirm you're looking at the right job (baseline vs. optimized).

**Live signal to catch:** If a job has 9 tasks running for a long time while 180 have already completed (as in a `180/423 (9 running)` pattern), those 9 tasks are likely skewed or stuck. Navigate immediately to the Stages tab to inspect them.

---

### Stages Tab — What to Track Live

Click into the active stage while it's running to see the **live task list**.

- **Task duration column:** Sort descending. If max duration is 10× median, you have skew.
- **Shuffle Read Size column:** Sort descending. If one task is reading far more bytes than others, that's the skewed key.
- **Locality Level column:** `PROCESS_LOCAL` = fastest (in-process memory). `NODE_LOCAL` = disk on same node. `RACK_LOCAL` or `ANY` = cross-rack network fetch. High `ANY` = scheduler couldn't place tasks close to data.
- **GC Time column:** Sort descending. Any task with GC time > 10% of its duration is under memory pressure.

---

### SQL Tab — What to Inspect After Plan Is Submitted

The SQL tab populates as soon as Spark submits the query plan (before tasks start).

- **Count Exchange nodes immediately.** Baseline should have more; optimized should have fewer or different types.
- **Find the join node type.** `SortMergeJoin` vs. `BroadcastHashJoin` is the most critical difference.
- **Check the FileScan node.** Look for `PartitionFilters` in the scan details. If empty, partition pruning is not working.
- **Look for AdaptiveSparkPlan wrapper.** If present, AQE is active. The plan may change between start and completion — check the final executed plan.
- **Row count estimates at each node.** If an estimate is wildly wrong (e.g., 50M rows estimated as 100 rows), the optimizer is flying blind — statistics are missing.

---

### Storage Tab — What to Track for Caching Experiments

- Before `.cache()`: tab is empty.
- After the first action that triggers caching: a new entry appears.
- Check `Fraction Cached` — it must reach 1.0 (100%) for the cache to be fully effective.
- Compare `Size in Memory` between `MEMORY_ONLY`, `MEMORY_AND_DISK`, and `DISK_ONLY` storage levels to measure serialization overhead.

---

### Executors Tab — Ongoing Health Check

Keep this tab open and check it every minute during long experiments.

- **Task Time (GC Time):** Should show GC as a small fraction of total.
- **Shuffle Read / Write per executor:** Should be roughly balanced. Large imbalance = skew.
- **Failed Tasks column:** Any failures? If so, check the executor logs (accessible from the executor ID link).
- **Storage Memory Used/Total:** If at 100%, the executor is under memory pressure and cached DataFrames may be evicted.

---

## Section 2 — What to Look For During Each Experiment Type

### Experiment 01 — Skewed Joins

**What's different between baseline and optimized:**

| Aspect | Baseline (Naive Join) | Optimized (Salted Join) |
|--------|----------------------|------------------------|
| Join type | SortMergeJoin | SortMergeJoin (but on salt+key) |
| Task distribution | Extreme right tail (1 straggler) | Roughly even histogram |
| Max task duration | Minutes | Similar to median |
| Shuffle read (max/median ratio) | > 100× | < 5× |
| Physical plan | Simple join on `account_id` | Extra `Project` (adding salt), join on `salt_account_id`, then re-join |

**What to observe:**
1. In the baseline: click into the join stage → sort tasks by shuffle read size → observe one task reading orders of magnitude more data.
2. In the optimized: observe the additional pre-join `Project` stage that adds a salt column. The join stage should show uniform task durations.
3. The salted plan will have more stages (one extra for the salt explode/replicate on the small side) but each stage will have balanced tasks.

---

### Experiment 02 — Broadcast Joins

**What's different:**

| Aspect | Baseline (SortMergeJoin) | Optimized (BroadcastHashJoin) |
|--------|--------------------------|-------------------------------|
| Exchange nodes | 2 (one per join side) | 1 BroadcastExchange (small side only) |
| Stage count | 3 (two shuffle stages + join) | 2 (scan/filter + join) |
| Shuffle Write (large side) | Hundreds of MB | 0 (no shuffle of large side) |
| Join node in SQL tab | `SortMergeJoin` | `BroadcastHashJoin` |
| Sort nodes | Yes (before each SMJ input) | None |

**What to observe:**
1. Baseline SQL tab: find two `Exchange hashpartitioning(...)` nodes feeding a `Sort` feeding `SortMergeJoin`.
2. Optimized SQL tab: find `BroadcastExchange` (small side only) feeding `BroadcastHashJoin`. No Exchange for the large side.
3. In Stages tab: baseline has a shuffle stage for the large table, optimized does not. This is the core speedup.
4. The overall job in the optimized case should have fewer stages — confirm this in the Jobs tab (`Stages: Succeeded/Total`).

---

### Experiment 03 — Partition Pruning

**What's different:**

| Aspect | Baseline (Full Scan) | Optimized (Pruned) |
|--------|---------------------|-------------------|
| FileScan partitions read | 30 (all 30 days) | 1–3 (only filtered dates) |
| Input size (Stages tab) | Full dataset | Fraction of dataset |
| PartitionFilters in SQL tab | Empty | `[event_date = 2024-01-15]` |
| Stage duration | Long | Short |
| Records read | All 50M | Subset |

**What to observe:**
1. In the SQL tab, click the `FileScan` node for both baseline and optimized.
2. Baseline: `PartitionFilters: []` — nothing pushed down.
3. Optimized: `PartitionFilters: [isnotnull(event_date#...), (event_date#... = 2024-01-15)]`.
4. In the Stages tab, compare `Input` column — the optimized stage should read a small fraction of the baseline input.
5. Note that `PushedFilters` (row-level filters) is different from `PartitionFilters` (directory-level filters). Both should be populated in a well-optimized plan.

---

### Experiment 04 — Caching Strategies

**What's different:**

| Aspect | No Cache | With Cache |
|--------|----------|-----------|
| Job count (for 3 consumers) | 3 × full DAG | 1 full DAG + 2 cheap reads |
| Storage tab entries | Empty | 1 entry per cached DF |
| Job duration (2nd and 3rd) | Same as 1st | Very fast (reads from memory) |
| Stage count per job | Full pipeline | 1–2 stages (just reads cached data) |

**What to observe:**
1. Without cache: run the experiment and count jobs in the Jobs tab. Three separate queries each show a full DAG (all the same stages replicated).
2. With `.cache()`: after the first action, go to Storage tab — confirm the entry appears with `Fraction Cached = 1.0`.
3. The 2nd and 3rd jobs should complete much faster with far fewer stages (the cached DataFrame replaces the entire upstream DAG).
4. In the SQL tab, the physical plan for cached queries shows `InMemoryTableScan` instead of `FileScan + Filter + ...`.
5. Compare `MEMORY_ONLY` vs. `DISK_ONLY` — Storage tab will show `Size in Memory = 0` for DISK_ONLY, and stage durations will be longer due to disk reads.

---

### Experiment 05 — Adaptive Query Execution

**What's different:**

| Aspect | AQE Off | AQE On |
|--------|---------|--------|
| Physical plan root node | Standard physical plan | `AdaptiveSparkPlan` |
| Post-shuffle partition count | Always `shuffle.partitions` (e.g., 200) | Fewer (coalesced small partitions) |
| Join type (if build side shrinks) | Stays SortMergeJoin | May upgrade to BroadcastHashJoin |
| Skewed join handling | One straggler task | Multiple balanced tasks |
| Task count in post-shuffle stages | Fixed 200 | Variable (e.g., 12 after coalescing) |

**What to observe:**
1. AQE Off: SQL tab shows a static physical plan. Stages tab shows exactly `shuffle.partitions` tasks in every post-shuffle stage.
2. AQE On: SQL tab shows `AdaptiveSparkPlan` at the root. After execution, expand the "Final Plan" section and compare it to the initial plan.
3. Look for `AQEShuffleRead` in the final plan — this replaces `Exchange` in the reduced-partition stages.
4. Count tasks in the post-shuffle stage with AQE on vs. off. You should see a dramatic reduction (e.g., 200 → 15 tasks).
5. For the skew sub-experiment: with AQE skew join on, look for `SkewJoin=true` annotation on the SortMergeJoin node and observe more tasks than `shuffle.partitions` in that stage (because skewed partitions were split).

---

### Experiment 06 — Deduplication Strategies

**What's different:**

| Aspect | dropDuplicates | row_number Window | Struct-max Aggregation |
|--------|---------------|-------------------|------------------------|
| Physical plan | Sort + Exchange + HashAggregate | Exchange + Sort + Window | Exchange + HashAggregate |
| Shuffle partitions | By all dedup columns | By partition-by columns | By dedup key |
| Determinism | Non-deterministic (any row kept) | Deterministic (specific row) | Deterministic |
| Exchange count | 1 | 1 | 1 |

**What to observe:**
1. In the SQL tab, look for `Window` node in the row_number plan. This confirms a window function is being used.
2. The window plan will show a `Sort` inside the window (for ordering within the partition) — this is CPU overhead vs. the simple aggregation approach.
3. Compare shuffle sizes between approaches — the struct-max aggregation may shuffle less data because aggregation can happen partially before the shuffle.
4. For production datasets with `~2%` duplicate rate, the struct-max approach should show the smallest shuffle write in the Stages tab.

---

### Experiment 07 — Shuffle Partition Tuning

**What's different:**

| Aspect | 200 partitions (default) | Tuned N | AQE auto-coalesce |
|--------|--------------------------|---------|-------------------|
| Task count in shuffle stages | 200 | N | Varies (< 200) |
| Per-task shuffle read size | Small if data is small | Target ~128MB | Auto-adjusted |
| Task overhead | High (many tiny tasks) | Lower | Lowest |
| Post-shuffle stage duration | High (scheduling overhead) | Balanced | Balanced |

**What to observe:**
1. With 200 partitions on a small dataset: Stages tab shows 200 tasks per shuffle stage, each processing maybe 500KB. The stage duration is dominated by task scheduling overhead, not actual computation.
2. With tuned N: Fewer tasks, each processing more data. Stage duration should decrease.
3. With AQE: The SQL tab shows `AQEShuffleRead` and the number of tasks adapts automatically. Check what the final coalesced partition count is — it should be far below 200 for your dataset size.
4. Key metric to track: `Shuffle Read Size (median per task)`. Target 100–200MB per task. Use this to back-calculate the right `shuffle.partitions` value.

---

### Experiment 08 — UDF vs Native Expressions

**What's different:**

| Aspect | Scala UDF | Native when/otherwise |
|--------|-----------|----------------------|
| WholeStageCodegen | Broken (UDF is opaque) | Intact |
| Physical plan | `BatchEvalPython` or deserialization overhead | Stays inside `*(...)` codegen node |
| CPU time | Higher (JVM object allocation, boxing) | Lower (tight generated loop) |
| Stage duration | Longer | Shorter |
| GC time | Higher | Lower |

**What to observe:**
1. In the SQL tab, look at the physical plan text (use `.explain("formatted")`).
2. Baseline UDF plan: the UDF computation appears outside `WholeStageCodegen` brackets (`*(N)`). You may see `DeserializeToObject`, `MapPartitions`, `SerializeFromObject` nodes.
3. Optimized plan: the `CASE WHEN ... THEN ...` logic is fused inside `*(N) Project [...]`. The entire operation is in one codegen block.
4. In the Stages tab, compare GC time between the two approaches. UDF version will typically show higher GC time because of boxing/unboxing and intermediate object allocation.
5. Also compare CPU time per task — the native expression version should have lower CPU time for the same number of records processed.

---

## Section 3 — Spark UI Investigation Workflow

Use this repeatable workflow for every experiment comparison. Apply it to both the baseline and the optimized run.

```
Step 1 — Open SQL tab → find the query → read the physical plan
         ↓
Step 2 — Count Exchange nodes
         (baseline: N exchanges → optimized: M exchanges, where M < N ideally)
         ↓
Step 3 — Identify join strategies
         Is it SMJ or BHJ?
         Is there a CartesianProduct (bad)?
         Is the BroadcastExchange on the correct (small) side?
         ↓
Step 4 — Check scan nodes
         Are PartitionFilters populated?
         Are PushedFilters populated?
         Are only needed columns listed in the schema?
         ↓
Step 5 — Go to Stages tab → find the slowest stage → click into it
         Look at task duration histogram
         Look at shuffle read size per task
         Is there skew? Spill? High GC?
         ↓
Step 6 — Check Executors tab
         Is GC time reasonable?
         Are shuffle read/write balanced across executors?
         Are any executors dead?
         ↓
Step 7 — Compare key metrics between baseline and optimized:
         □ Total job duration
         □ Slowest stage duration
         □ Total shuffle write size
         □ Max task duration / Median task duration (skew ratio)
         □ Number of stages
         □ Exchange node count in physical plan
         □ Partition count in post-shuffle stages
```

**Document your findings:** For each experiment, write down the before/after values of these metrics. This creates an empirical record of what each optimization actually achieved.

---

## Section 4 — Spark UI Signals That Indicate Problems

### Signal: Huge Shuffle Write

**What you see:** Stages tab → Shuffle Write column shows GBs for a stage where you expected MBs.

**What it means:** Either the data is genuinely large, or there's no pre-aggregation happening before the shuffle (no partial HashAggregate), or you're shuffling unnecessarily wide (too many columns in the join/group key).

**Fix:**
- Check SQL tab for `HashAggregate` (partial) before Exchange — if absent, Spark couldn't do pre-agg (e.g., because of `DISTINCT` semantics)
- Check if you can reduce the columns in the shuffle key
- Consider broadcasting the smaller side

---

### Signal: One Task Running Much Longer (Straggler)

**What you see:** Stages tab → task duration histogram has a long right tail; one or a few tasks take 10–100× longer than the median.

**What it means:** Data skew. One partition has far more data than others, either due to a skewed key distribution or a small number of very hot keys.

**Fix:**
- Enable AQE skew join optimization (`spark.sql.adaptive.skewJoin.enabled = true`)
- Salt the skewed join key manually (Experiment 01)
- Filter hot keys and process them separately with a union

---

### Signal: High GC Time

**What you see:** Stages tab → task detail → GC time is > 10% of task time; or Executors tab → GC Time column is high for specific executors.

**What it means:** Partitions are too large for the executor heap, causing frequent garbage collection. Or large objects (DataFrames, arrays) are being held in task closures.

**Fix:**
- Increase partition count to reduce per-partition size
- Increase `spark.executor.memory` or `spark.executor.memoryOverhead`
- Check for closures capturing large objects in transformations

---

### Signal: Too Many Tiny Tasks

**What you see:** Stages tab → 200 tasks, each processing 200KB, completing in 10ms each. Stage duration is dominated by scheduling overhead.

**What it means:** `spark.sql.shuffle.partitions` is too high for your data volume, or you have a small dataset with a fixed high partition count.

**Fix:**
- Lower `spark.sql.shuffle.partitions` to match your data volume
- Enable AQE partition coalescing
- Use `.coalesce(N)` after large filters that reduce data volume significantly

---

### Signal: Missing Broadcast Join

**What you see:** SQL tab shows `SortMergeJoin` where you expected `BroadcastHashJoin`. The small dimension table is being shuffled unnecessarily.

**What it means:** Optimizer estimated the table as too large to broadcast, or `autoBroadcastJoinThreshold` is set too low, or broadcasts are disabled (`= -1`).

**Fix:**
- Check `spark.sql.autoBroadcastJoinThreshold` in Environment tab
- Use `broadcast()` hint: `largeDF.join(broadcast(smallDF), "key")`
- Run `ANALYZE TABLE` on the small table to update statistics
- Enable AQE (it may upgrade to BHJ at runtime based on actual sizes)

---

### Signal: Missing Partition Pruning

**What you see:** SQL tab → FileScan node → `PartitionFilters: []` is empty even though you filtered on a partition column. Stages tab → Input size matches the full dataset.

**What it means:** Spark didn't push your filter to the file scan level. The filter is applied after reading all files.

**Fix:**
- Ensure you're filtering on the actual partition column (check column name carefully)
- Avoid applying UDFs to partition columns in filter conditions
- Avoid wrapping partition columns in expressions: `year(event_date) = 2024` won't prune — use `event_date >= '2024-01-01' AND event_date < '2025-01-01'`
- Check if Dynamic Partition Pruning is expected — it requires AQE and specific join patterns

---

### Signal: Codegen Disabled

**What you see:** SQL tab physical plan — operations appear outside `*(N) WholeStageCodegen` blocks. You see `DeserializeToObject`, `MapElements`, `SerializeFromObject` nodes.

**What it means:** A UDF, Python operation, or non-codegen-compatible operation broke the WholeStageCodegen boundary. Everything outside the codegen block is interpreted, not JIT-compiled.

**Fix:**
- Replace UDFs with native Spark functions (`when`, `otherwise`, `regexp_extract`, etc.)
- For Scala UDFs that can't be eliminated, consider using Catalyst expressions via `ExpressionEncoder` or contributing a custom Expression
- Use `spark.sql.codegen.wholeStage = true` (default) and verify it's not disabled

---

## Section 5 — Deep Learning Exercise Checklist

Run this checklist for **every experiment** — both the baseline run and the optimized run. Fill in the values in a notebook or markdown file.

### Before Running

```
□ I have the Spark UI open at http://localhost:4040
□ I have Jobs, Stages, SQL, and Executors tabs open in separate browser tabs
□ I know what optimization this experiment is testing
□ I have a prediction: what should change in the plan and metrics?
```

### During Execution

```
□ Jobs tab: How many stages does this job have? (write the number: ___)
□ Jobs tab: Is the progress bar stalling anywhere? At what % (___)?
□ Stages tab (active): How many tasks are running vs. complete?
□ Stages tab (active): Are any tasks running dramatically longer than others?
□ Executors tab: Is GC time visible? Which executor has the highest? (__%)
```

### After Completion — SQL Tab Investigation

```
□ Count total Exchange nodes in the physical plan: ___
□ Identify join type(s): SMJ / BHJ / SHJ / Cartesian (circle)
□ Is BroadcastExchange present? On which side? (large/small)
□ Check FileScan node — PartitionFilters populated? Yes / No
□ Check FileScan node — PushedFilters populated? Yes / No
□ Is AdaptiveSparkPlan at the root? Yes / No
□ Is WholeStageCodegen intact (no UDF nodes outside *() blocks)? Yes / No
□ Estimated vs. actual row counts at the join node — are they close? Yes / No
```

### After Completion — Stages Tab Investigation

```
□ Which stage took longest? Stage ID: ___, Duration: ___
□ How many tasks did it have? ___
□ Median task duration: ___, Max task duration: ___, Ratio (max/median): ___
□ Shuffle Write (this stage): ___, Shuffle Read (next stage): ___
□ Spill (Disk): ___ (should be 0)
□ Is the task duration histogram narrow (healthy) or right-tailed (skewed)?
```

### After Completion — Comparison Table

Fill this in for both baseline and optimized:

| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Total job duration | | | |
| Number of stages | | | |
| Number of Exchange nodes | | | |
| Join strategy | | | |
| Slowest stage duration | | | |
| Total shuffle write | | | |
| Max/Median task ratio | | | |
| Partition count (post-shuffle) | | | |
| Spill (Disk) | | | |

### Reflection Questions (Write Answers After Each Experiment)

1. What did the optimized plan eliminate that the baseline plan had? (Exchanges? Sorts? Full scans?)
2. What was the single biggest contributor to the speedup?
3. Did the actual plan match what you predicted? If not, why?
4. What configuration setting controlled the key behavior you observed?
5. How would you apply this optimization in a production pipeline?
6. What could make this optimization fail at larger scale?

---

*This playbook is designed to be used alongside `sparkuimanual.md`. Refer to the manual for concept definitions; use this playbook as your active guide during experiment execution.*
