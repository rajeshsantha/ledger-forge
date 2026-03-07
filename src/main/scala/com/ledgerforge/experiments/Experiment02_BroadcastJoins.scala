package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

/**
 * ============================================================================
 * Experiment 02 — Broadcast Joins
 * ============================================================================
 *
 * CONCEPT:
 *   When one side of a join is small enough to fit in driver/executor memory,
 *   Spark can "broadcast" it — serialise the entire DataFrame and ship it to
 *   every executor.  Each executor then does a local hash-map lookup instead
 *   of a full shuffle.
 *
 * BASELINE:
 *   Default join strategy: Spark picks SortMergeJoin for two large-looking
 *   DataFrames.  Both sides get shuffled (2 Exchange nodes).
 *
 * OPTIMIZED:
 *   `broadcast(product)` hint — Spark broadcasts the small product table
 *   (5 000 rows, ~100 KB) and performs a BroadcastHashJoin.
 *   Result: 0 Exchange nodes on the large side; only the broadcast exchange.
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • SQL tab → BroadcastHashJoin replaces SortMergeJoin.
 *   • Stages tab → the broadcast join has fewer stages (no shuffle on the
 *     large transaction side).
 *   • Jobs tab → significant wall-clock improvement.
 *
 * DAG CHANGES:
 *   Baseline: Scan(txn) → Exchange → Sort → SortMergeJoin ← Sort ← Exchange ← Scan(product)
 *   Optimized: Scan(txn) → BroadcastHashJoin ← BroadcastExchange ← Scan(product)
 *
 * SPARK CONFIG:
 *   `spark.sql.autoBroadcastJoinThreshold` (default 10MB) controls when
 *   Spark auto-broadcasts.  Product (5K rows) is well under this limit so
 *   Spark may auto-broadcast even without the hint.  We disable auto-broadcast
 *   in the baseline to force SortMergeJoin.
 * ============================================================================
 */
object Experiment02_BroadcastJoins {

  def main(args: Array[String]): Unit = {
    ExperimentUtils.printHeader("Experiment 02 — Broadcast Joins")

    // ========================================================================
    // BASELINE — force SortMergeJoin by disabling auto-broadcast
    // ========================================================================
    ExperimentUtils.printSubHeader("BASELINE — SortMergeJoin (auto-broadcast disabled)")

    val sparkNoBC = ExperimentUtils.createSpark("Exp02_NoBroadcast", Map(
      "spark.sql.autoBroadcastJoinThreshold" -> "-1"  // disable auto-broadcast
    ))

    val txn1     = sparkNoBC.read.parquet(ExperimentUtils.TransactionPath)
    val product1 = sparkNoBC.read.parquet(ExperimentUtils.ProductPath)

    val baselineJoin = txn1.join(product1, Seq("product_id"), "left")
      .withColumn("product_name", coalesce(col("product_name"), lit("Unknown")))
      .withColumn("product_group", coalesce(col("product_group"), lit("Unknown")))

    val baselinePlan = ExperimentUtils.captureExplain(baselineJoin)
    println(baselinePlan)

    val (baselineCount, baselineMs) = {
      val start = System.nanoTime()
      val cnt = baselineJoin.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }
    println(s"Baseline count     : $baselineCount")
    println(s"Baseline time      : ${baselineMs} ms")
    println(s"Baseline exchanges : ${ExperimentUtils.countExchanges(baselinePlan)}")

    ExperimentUtils.resetSpark()

    // ========================================================================
    // OPTIMIZED — explicit broadcast hint
    // ========================================================================
    ExperimentUtils.printSubHeader("OPTIMIZED — BroadcastHashJoin (explicit hint)")

    val sparkBC = ExperimentUtils.createSpark("Exp02_Broadcast")

    val txn2     = sparkBC.read.parquet(ExperimentUtils.TransactionPath)
    val product2 = sparkBC.read.parquet(ExperimentUtils.ProductPath)

    val broadcastJoin = txn2.join(broadcast(product2), Seq("product_id"), "left")
      .withColumn("product_name", coalesce(col("product_name"), lit("Unknown")))
      .withColumn("product_group", coalesce(col("product_group"), lit("Unknown")))

    val broadcastPlan = ExperimentUtils.captureExplain(broadcastJoin)
    println(broadcastPlan)

    val (bcCount, bcMs) = {
      val start = System.nanoTime()
      val cnt = broadcastJoin.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }
    println(s"Broadcast count     : $bcCount")
    println(s"Broadcast time      : ${bcMs} ms")
    println(s"Broadcast exchanges : ${ExperimentUtils.countExchanges(broadcastPlan)}")

    // ========================================================================
    // AUTO-BROADCAST — let Spark decide (default threshold = 10MB)
    // ========================================================================
    ExperimentUtils.printSubHeader("AUTO-BROADCAST — Spark decides (default threshold)")

    val autoJoin = txn2.join(product2, Seq("product_id"), "left")
    val autoPlan = ExperimentUtils.captureExplain(autoJoin)
    val isBroadcast = autoPlan.contains("BroadcastHashJoin") || autoPlan.contains("BroadcastExchange")
    println(s"Auto-broadcast picked BroadcastHashJoin? $isBroadcast")
    println(s"Product table row count: ${product2.count()}")

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Approach", "Count", "Time (ms)", "Exchanges", "Join Strategy"),
      Seq(
        Seq("SortMergeJoin", baselineCount.toString, baselineMs.toString,
          ExperimentUtils.countExchanges(baselinePlan).toString, "SortMergeJoin"),
        Seq("BroadcastHashJoin", bcCount.toString, bcMs.toString,
          ExperimentUtils.countExchanges(broadcastPlan).toString, "BroadcastHashJoin")
      )
    )

    println(
      """
        |KEY TAKEAWAYS:
        |  • BroadcastHashJoin eliminates the shuffle on the large (transaction) side.
        |  • The small table (product: 5K rows) is serialised once and sent to all
        |    executors — each executor builds a local hash map for O(1) lookups.
        |  • SortMergeJoin requires both sides to be shuffled and sorted — expensive
        |    for 50M rows even when the other side is tiny.
        |  • `spark.sql.autoBroadcastJoinThreshold` (default 10MB) means Spark will
        |    auto-broadcast tables under that size.  Explicit `broadcast()` overrides
        |    this regardless of estimated size.
        |  • Rule of thumb: always broadcast dimension tables (< 100MB).
        |  • Watch Spark UI → SQL tab → look for BroadcastExchange node.
        |""".stripMargin)

    sparkBC.stop()
  }
}

