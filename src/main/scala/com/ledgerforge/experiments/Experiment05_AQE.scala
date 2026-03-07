package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions._

/**
 * ============================================================================
 * Experiment 05 — Adaptive Query Execution (AQE)
 * ============================================================================
 *
 * CONCEPT:
 *   AQE (Spark 3.x, enabled by default since 3.2) re-optimises the query plan
 *   at runtime based on actual shuffle statistics.  It can:
 *     1. Coalesce shuffle partitions — reduce 200 → actual needed count.
 *     2. Switch join strategy — convert SortMergeJoin → BroadcastHashJoin
 *        if the actual size of one side is small enough.
 *     3. Handle skew — split oversized partitions into sub-partitions.
 *
 * THIS EXPERIMENT RUNS THE SAME PIPELINE 3 TIMES:
 *   Config A: AQE OFF (baseline)
 *   Config B: AQE ON  (coalescing + strategy switching)
 *   Config C: AQE ON + skew join optimization
 *
 * PIPELINE:
 *   transaction → join(account, "account_id") → groupBy("branch_id").agg(sum)
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • SQL tab → look for `AdaptiveSparkPlan` as the root node when AQE is on.
 *   • Stages tab → with AQE, the actual number of tasks after coalescing is
 *     much less than `spark.sql.shuffle.partitions` (200).
 *   • In Config C, look for "skew join" annotations in the plan.
 *   • Compare: total tasks, stages, and wall-clock time.
 *
 * DAG CHANGES:
 *   AQE OFF:  Static plan with 200 shuffle partitions; straggler tasks on
 *             skewed account_id partitions.
 *   AQE ON:   Runtime coalescing reduces empty/tiny partitions; may switch
 *             to BroadcastHashJoin if account table is small enough.
 *   AQE+SKEW: Additionally splits hot partitions into smaller chunks.
 * ============================================================================
 */
object Experiment05_AQE {

  def main(args: Array[String]): Unit = {
    ExperimentUtils.printHeader("Experiment 05 — Adaptive Query Execution (AQE)")

    val results = scala.collection.mutable.ArrayBuffer[(String, Long, Int, String)]()

    // ========================================================================
    // Config A: AQE OFF
    // ========================================================================
    ExperimentUtils.printSubHeader("Config A — AQE OFF")
    val (countA, msA, exchangesA, joinStrategyA) = runPipeline(
      "Exp05_AQE_OFF",
      Map(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.shuffle.partitions" -> "200"
      )
    )
    results += (("AQE OFF", msA, exchangesA, joinStrategyA))

    // ========================================================================
    // Config B: AQE ON (default coalescing + strategy switching)
    // ========================================================================
    ExperimentUtils.printSubHeader("Config B — AQE ON")
    val (countB, msB, exchangesB, joinStrategyB) = runPipeline(
      "Exp05_AQE_ON",
      Map(
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
        "spark.sql.adaptive.skewJoin.enabled" -> "false",  // skew off for this config
        "spark.sql.shuffle.partitions" -> "200"
      )
    )
    results += (("AQE ON", msB, exchangesB, joinStrategyB))

    // ========================================================================
    // Config C: AQE ON + Skew Join Optimization
    // ========================================================================
    ExperimentUtils.printSubHeader("Config C — AQE ON + Skew Join Optimization")
    val (countC, msC, exchangesC, joinStrategyC) = runPipeline(
      "Exp05_AQE_SKEW",
      Map(
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
        "spark.sql.adaptive.skewJoin.enabled" -> "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor" -> "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "67108864", // 64MB
        "spark.sql.shuffle.partitions" -> "200"
      )
    )
    results += (("AQE ON + Skew", msC, exchangesC, joinStrategyC))

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Config", "Time (ms)", "Exchanges", "Join Strategy"),
      results.map { case (name, ms, ex, js) =>
        Seq(name, ms.toString, ex.toString, js)
      }
    )

    println(
      """
        |KEY TAKEAWAYS:
        |  • AQE dynamically reduces shuffle partitions from 200 to the actual
        |    needed count (often 20-50 for this dataset on local mode).
        |  • With AQE + skew join, Spark detects oversized partitions and splits
        |    them automatically — no manual salting required.
        |  • AQE can also switch SortMergeJoin → BroadcastHashJoin at runtime
        |    if it discovers one side is small enough after the shuffle.
        |
        |KEY CONFIGS:
        |  spark.sql.adaptive.enabled = true                         (master switch)
        |  spark.sql.adaptive.coalescePartitions.enabled = true      (merge small partitions)
        |  spark.sql.adaptive.skewJoin.enabled = true                (split hot partitions)
        |  spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5     (5× median = skewed)
        |  spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 64MB
        |  spark.sql.adaptive.coalescePartitions.minPartitionSize = 1MB
        |
        |  • coalescePartitions.minPartitionSize prevents over-coalescing into
        |    too-large partitions — useful when you want a minimum level of
        |    parallelism.
        |
        |AQE vs MANUAL SALTING (Experiment 01):
        |  • AQE is zero-code — just enable configs.
        |  • Salting gives you fine-grained control and works on older Spark.
        |  • AQE skew join only handles equi-join skew; salting works for any skew.
        |""".stripMargin)
  }

  private def runPipeline(
      appName: String,
      conf: Map[String, String]
  ): (Long, Long, Int, String) = {
    ExperimentUtils.resetSpark()
    val spark = ExperimentUtils.createSpark(appName, conf)

    val transactions = spark.read.parquet(ExperimentUtils.TransactionPath)
    val accounts     = spark.read.parquet(ExperimentUtils.AccountPath)

    val pipeline: org.apache.spark.sql.DataFrame = transactions
      .join(accounts.drop("branch_id"), Seq("account_id"), "inner")
      .groupBy("branch_id")
      .agg(
        F.sum("amount").as("total_amount"),
        F.count(lit(1)).as("txn_count")
      )

    // Capture plan
    val plan = ExperimentUtils.captureExplain(pipeline)
    println(plan)

    val exchanges = ExperimentUtils.countExchanges(plan)
    val joinStrategy = if (plan.contains("BroadcastHashJoin")) "BroadcastHashJoin"
                       else if (plan.contains("SortMergeJoin")) "SortMergeJoin"
                       else if (plan.contains("ShuffledHashJoin")) "ShuffledHashJoin"
                       else "Unknown"

    val isAdaptive = plan.contains("AdaptiveSparkPlan")
    println(s"Adaptive plan? $isAdaptive")

    // Execute
    val start = System.nanoTime()
    val result = pipeline.collect()
    val ms = (System.nanoTime() - start) / 1000000
    val count = result.length.toLong

    println(s"Branch count: $count, Time: ${ms} ms")

    // Print partition info
    ExperimentUtils.printPartitionInfo(appName, pipeline)

    spark.stop()
    (count, ms, exchanges, joinStrategy)
  }
}

