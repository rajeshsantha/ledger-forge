package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * ============================================================================
 * Experiment 04 — Caching Strategies
 * ============================================================================
 *
 * CONCEPT:
 *   When a DataFrame is used by multiple downstream consumers (actions),
 *   Spark recomputes it from scratch each time unless you cache/persist it.
 *   Different caching strategies trade off memory, disk I/O, and lineage.
 *
 * TEST SETUP:
 *   1. Build a deduplicated transaction DataFrame (expensive: window + filter).
 *   2. Run 3 downstream actions on it:
 *      a) count()
 *      b) groupBy("currency").agg(sum("amount"))
 *      c) filter(is_valid = false).count()
 *
 * STRATEGIES COMPARED:
 *   1. No caching — Spark recomputes the dedup for each action (3× compute).
 *   2. .cache() — MEMORY_AND_DISK (default in Spark 3.x).
 *   3. .persist(DISK_ONLY) — useful when memory is tight.
 *   4. .checkpoint() — breaks lineage; writes to reliable storage.
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • Storage tab → cached RDDs, memory/disk usage per strategy.
 *   • Jobs tab → no-cache has 3 separate jobs computing the same DAG;
 *     cached has 1 compute job + 2 cache-hit jobs.
 *   • DAG visualization → checkpoint breaks the lineage graph.
 *
 * METRICS TO COMPARE:
 *   Total wall-clock time for all 3 actions under each strategy.
 * ============================================================================
 */
object Experiment04_CachingStrategies {

  def main(args: Array[String]): Unit = {
    val spark = ExperimentUtils.createSpark("Exp04_Caching", Map(
      "spark.sql.adaptive.enabled" -> "false"  // disable AQE for fair comparison
    ))
    import spark.implicits._

    // Checkpoint directory (required for strategy 4)
    spark.sparkContext.setCheckpointDir("output/checkpoints/exp04")

    ExperimentUtils.printHeader("Experiment 04 — Caching Strategies")

    // ------ Build the expensive dedup DataFrame ------
    val raw = spark.read.parquet(ExperimentUtils.TransactionPath)

    // Window-based dedup (same as Problem 4)
    val w = Window.partitionBy("transaction_id").orderBy(col("ingest_date").desc, col("event_ts").desc)

    def buildDedup() = {
      raw
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") === 1)
        .drop("rn")
    }

    // ------ 3 downstream actions ------
    def runActions(label: String, df: org.apache.spark.sql.DataFrame): Long = {
      val start = System.nanoTime()

      // Action 1: count
      val cnt = df.count()
      println(s"  [$label] Action 1 — count: $cnt")

      // Action 2: aggregate
      val aggResult = df.groupBy("currency").agg(
        F.sum("amount").as("total"),
        F.count(lit(1)).as("txn_count")
      ).collect()
      println(s"  [$label] Action 2 — currencies aggregated: ${aggResult.length}")

      // Action 3: filter + count
      val invalidCount = df.filter(col("account_id") <= 0 || col("account_id").isNull).count()
      println(s"  [$label] Action 3 — invalid account rows: $invalidCount")

      val totalMs = (System.nanoTime() - start) / 1000000
      totalMs
    }

    // ========================================================================
    // Strategy 1: NO CACHING
    // ========================================================================
    ExperimentUtils.printSubHeader("Strategy 1 — No Caching (recompute 3×)")

    val noCacheMs = {
      val dedup = buildDedup()
      runActions("NoCache", dedup)
    }
    println(s"  Total time (no cache): ${noCacheMs} ms\n")

    // ========================================================================
    // Strategy 2: .cache() — MEMORY_AND_DISK
    // ========================================================================
    ExperimentUtils.printSubHeader("Strategy 2 — .cache() [MEMORY_AND_DISK]")

    val cacheMs = {
      val dedup = buildDedup().cache()
      val ms = runActions("Cache", dedup)
      dedup.unpersist(blocking = true)
      ms
    }
    println(s"  Total time (cache): ${cacheMs} ms\n")

    // ========================================================================
    // Strategy 3: .persist(DISK_ONLY)
    // ========================================================================
    ExperimentUtils.printSubHeader("Strategy 3 — .persist(DISK_ONLY)")

    val diskMs = {
      val dedup = buildDedup().persist(StorageLevel.DISK_ONLY)
      val ms = runActions("DiskOnly", dedup)
      dedup.unpersist(blocking = true)
      ms
    }
    println(s"  Total time (disk only): ${diskMs} ms\n")

    // ========================================================================
    // Strategy 4: .checkpoint() — breaks lineage
    // ========================================================================
    ExperimentUtils.printSubHeader("Strategy 4 — .checkpoint() [eager, breaks lineage]")

    val checkpointMs = {
      val dedup = buildDedup().checkpoint(eager = true) // triggers computation immediately
      val ms = runActions("Checkpoint", dedup)
      ms
    }
    println(s"  Total time (checkpoint): ${checkpointMs} ms\n")

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Strategy", "Total Time (ms)", "Recomputes Dedup?", "Memory Use", "Lineage"),
      Seq(
        Seq("No Caching", noCacheMs.toString, "3× (once per action)", "None", "Full"),
        Seq(".cache()", cacheMs.toString, "1× (then cache hit)", "High", "Full"),
        Seq(".persist(DISK_ONLY)", diskMs.toString, "1× (then disk read)", "None (disk)", "Full"),
        Seq(".checkpoint()", checkpointMs.toString, "1× (eager write)", "None (disk)", "Truncated")
      )
    )

    println(
      """
        |KEY TAKEAWAYS:
        |  • No caching recomputes the expensive window-based dedup 3 times.
        |  • .cache() is fastest for repeated reads if data fits in memory.
        |  • .persist(DISK_ONLY) is useful when memory is limited — slower than
        |    memory cache but still avoids recomputation.
        |  • .checkpoint() is special: it eagerly materialises and BREAKS the
        |    lineage.  This is useful when the DAG is very deep (hundreds of
        |    transformations) causing StackOverflowError during planning.
        |  • Always call .unpersist() when done to free memory/disk.
        |  • In Spark UI → Storage tab, you can see cached DataFrames and their
        |    memory/disk footprint.
        |
        |WHEN TO USE EACH:
        |  • .cache()      → multiple actions on same DF, data fits in cluster memory
        |  • .persist(DISK) → same as above but memory-constrained
        |  • .checkpoint()  → very deep lineage, or you need fault-tolerant materialisation
        |  • No caching     → one-shot DF used only once (caching adds overhead)
        |""".stripMargin)

    spark.stop()
  }
}




