package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions._

/**
 * ============================================================================
 * Experiment 07 — Shuffle Partition Tuning
 * ============================================================================
 *
 * CONCEPT:
 *   `spark.sql.shuffle.partitions` (default 200) controls how many partitions
 *   are created after every shuffle (join, groupBy, window, etc.).
 *
 *   Too few partitions → each task processes too much data → OOM or slow tasks.
 *   Too many partitions → scheduling overhead, too many small files, under-utilised cores.
 *
 *   The optimal value depends on:
 *     • Total data size after shuffle
 *     • Number of cores available
 *     • Target partition size (128MB–256MB is a good rule of thumb)
 *
 * THIS EXPERIMENT:
 *   Runs the same join+aggregate pipeline with shuffle.partitions set to:
 *   20, 50, 200 (default), 500, 1000.
 *   Measures wall-clock time and output partition count for each.
 *
 * BONUS:
 *   Runs with AQE auto-coalescing (which dynamically adjusts partitions)
 *   and compares the result.
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • Stages tab → task count = shuffle.partitions for each shuffle stage.
 *   • With too few partitions, each task is slow (GC pressure, spill to disk).
 *   • With too many, tasks finish in <100ms but scheduling overhead dominates.
 *   • AQE auto-coalescing finds a middle ground automatically.
 * ============================================================================
 */
object Experiment07_ShufflePartitionTuning {

  private val PartitionCounts = Seq(20, 50, 200, 500, 1000)

  def main(args: Array[String]): Unit = {
    ExperimentUtils.printHeader("Experiment 07 — Shuffle Partition Tuning")

    val results = scala.collection.mutable.ArrayBuffer[(String, Long, Long, Int)]()

    // ========================================================================
    // Run with fixed partition counts
    // ========================================================================
    PartitionCounts.foreach { numPartitions =>
      ExperimentUtils.printSubHeader(s"shuffle.partitions = $numPartitions")
      val (count, ms, actualPartitions) = runPipeline(
        s"Exp07_P$numPartitions",
        Map(
          "spark.sql.adaptive.enabled" -> "false",
          "spark.sql.shuffle.partitions" -> numPartitions.toString
        )
      )
      results += ((s"Fixed=$numPartitions", count, ms, actualPartitions))
    }

    // ========================================================================
    // Run with AQE auto-coalescing (starts at 200, coalesces dynamically)
    // ========================================================================
    ExperimentUtils.printSubHeader("AQE Auto-Coalescing (initial=200, dynamic)")
    val (countAQE, msAQE, actualAQE) = runPipeline(
      "Exp07_AQE",
      Map(
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> "200",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "1MB",
        "spark.sql.shuffle.partitions" -> "200"
      )
    )
    results += (("AQE Auto", countAQE, msAQE, actualAQE))

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Config", "Result Rows", "Time (ms)", "Output Partitions"),
      results.map { case (name, count, ms, parts) =>
        Seq(name, count.toString, ms.toString, parts.toString)
      }
    )

    println(
      """
        |KEY TAKEAWAYS:
        |  • Too few partitions (20): tasks are slow, possible spill to disk.
        |    Each task processes ~2.5M rows = lots of data per core.
        |  • Too many partitions (1000): scheduling overhead, many tiny tasks.
        |    With 50M rows / 1000 = 50K rows per task = underutilised.
        |  • Sweet spot depends on cluster size and data volume.
        |    Rule of thumb: target 128MB–256MB per partition after shuffle.
        |
        |FORMULA:
        |  optimal_partitions = total_shuffle_data_size / target_partition_size
        |  Example: 50M rows × 100 bytes = 5GB → 5GB / 128MB ≈ 40 partitions
        |
        |AQE AUTO-COALESCING:
        |  • Starts with spark.sql.shuffle.partitions (200) as the upper bound.
        |  • After the shuffle, inspects actual partition sizes.
        |  • Merges small adjacent partitions until each is ≥ minPartitionSize.
        |  • No manual tuning needed — best default for production.
        |
        |SPARK UI OBSERVATIONS:
        |  • With 20 partitions: few tasks, each with high GC time.
        |  • With 1000 partitions: many tasks, each finishes instantly but
        |    the scheduler overhead is visible in the timeline.
        |  • AQE: starts at 200 tasks in the plan, but the actual number of
        |    post-coalesce tasks (visible in Stages tab) is lower.
        |""".stripMargin)
  }

  private def runPipeline(
      appName: String,
      conf: Map[String, String]
  ): (Long, Long, Int) = {
    ExperimentUtils.resetSpark()
    val spark = ExperimentUtils.createSpark(appName, conf)

    val transactions = spark.read.parquet(ExperimentUtils.TransactionPath)
    val accounts     = spark.read.parquet(ExperimentUtils.AccountPath)

    val pipeline: org.apache.spark.sql.DataFrame = transactions
      .filter(col("account_id").isNotNull && col("account_id") > 0)
      .join(accounts, Seq("account_id"), "inner")
      .groupBy("branch_id", "account_type")
      .agg(
        F.sum("amount").as("total_amount"),
        F.count(lit(1)).as("txn_count"),
        F.avg("amount").as("avg_amount")
      )
      .orderBy(desc("total_amount"))

    val start = System.nanoTime()
    val result = pipeline.collect()
    val ms = (System.nanoTime() - start) / 1000000
    val count = result.length.toLong

    val actualPartitions = pipeline.rdd.getNumPartitions
    println(s"Result rows: $count, Time: ${ms} ms, Partitions: $actualPartitions")

    spark.stop()
    (count, ms, actualPartitions)
  }
}





