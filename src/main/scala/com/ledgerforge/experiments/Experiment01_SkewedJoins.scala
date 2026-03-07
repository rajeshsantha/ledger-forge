package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

/**
 * ============================================================================
 * Experiment 01 — Skewed Joins
 * ============================================================================
 *
 * CONCEPT:
 *   The transaction dataset has a Zipf-distributed `account_id` — a handful
 *   of "hot" accounts concentrate millions of rows while most accounts have
 *   very few.  A regular equi-join on `account_id` hashes every row to a
 *   partition based on the key; hot keys land in the same partition, creating
 *   a straggler task that dominates wall-clock time.
 *
 * BASELINE:
 *   Naive `transaction.join(account, "account_id")`.
 *
 * OPTIMIZED:
 *   "Salted join" — append a random salt (0 to N-1) to each transaction row,
 *   replicate the account dimension N times (once per salt value), and join on
 *   (account_id, salt).  This spreads hot-key rows across N partitions.
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • Stages tab → task duration distribution: baseline will show a few tasks
 *     10-100× slower than the median.  Salted version should be uniform.
 *   • SQL tab → Exchange node sizes before and after.
 *   • Jobs tab → total wall-clock time difference.
 *
 * DAG CHANGES:
 *   Baseline: 2 Exchanges (shuffle both sides) → SortMergeJoin.
 *   Salted:   2 Exchanges on (account_id, salt) → SortMergeJoin, but each
 *             partition is smaller because the hot key is split N ways.
 *             The account side is N× larger (replicated), but it's a small
 *             dimension so the trade-off is worthwhile.
 * ============================================================================
 */
object Experiment01_SkewedJoins {

  private val SaltBuckets = 10

  def main(args: Array[String]): Unit = {
    val spark = ExperimentUtils.createSpark("Exp01_SkewedJoins")
    import spark.implicits._

    ExperimentUtils.printHeader("Experiment 01 — Skewed Joins")

    // ------ Load data ------
    val transactions = spark.read.parquet(ExperimentUtils.TransactionPath)
    val accounts     = spark.read.parquet(ExperimentUtils.AccountPath)

    // Show the skew: top 10 account_ids by frequency
    ExperimentUtils.printSubHeader("Account-ID frequency — top 10 (Zipf skew)")
    transactions.groupBy("account_id").count()
      .orderBy(desc("count"))
      .show(10, truncate = false)

    // ========================================================================
    // BASELINE — naive join
    // ========================================================================
    ExperimentUtils.printSubHeader("BASELINE — Naive Join")

    val (baselineCount, baselineMs) = {
      val start = System.nanoTime()
      val joined = transactions.join(accounts, Seq("account_id"), "inner")
      val cnt = joined.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }

    // Capture and print the physical plan
    val baselinePlan = ExperimentUtils.captureExplain(
      transactions.join(accounts, Seq("account_id"), "inner")
    )
    println(baselinePlan)
    println(s"Baseline count : $baselineCount")
    println(s"Baseline time  : ${baselineMs} ms")
    println(s"Exchanges      : ${ExperimentUtils.countExchanges(baselinePlan)}")

    // ========================================================================
    // OPTIMIZED — salted join
    // ========================================================================
    ExperimentUtils.printSubHeader("OPTIMIZED — Salted Join (salt = $SaltBuckets)")

    val (saltedCount, saltedMs) = {
      val start = System.nanoTime()

      // 1. Add a random salt column to the large (transaction) side
      val txnSalted = transactions.withColumn("salt", (rand() * SaltBuckets).cast("int"))

      // 2. Explode the small (account) side: replicate each row N times
      val accountExploded = accounts
        .withColumn("salt", explode(array((0 until SaltBuckets).map(lit(_)): _*)))

      // 3. Join on the composite key (account_id, salt)
      val joined = txnSalted.join(accountExploded, Seq("account_id", "salt"), "inner")
        .drop("salt")

      val cnt = joined.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }

    val saltedPlan = {
      val txnSalted = transactions.withColumn("salt", (rand() * SaltBuckets).cast("int"))
      val accountExploded = accounts
        .withColumn("salt", explode(array((0 until SaltBuckets).map(lit(_)): _*)))
      ExperimentUtils.captureExplain(
        txnSalted.join(accountExploded, Seq("account_id", "salt"), "inner").drop("salt")
      )
    }
    println(saltedPlan)
    println(s"Salted count   : $saltedCount")
    println(s"Salted time    : ${saltedMs} ms")
    println(s"Exchanges      : ${ExperimentUtils.countExchanges(saltedPlan)}")

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Approach", "Count", "Time (ms)", "Exchanges"),
      Seq(
        Seq("Naive Join", baselineCount.toString, baselineMs.toString,
          ExperimentUtils.countExchanges(baselinePlan).toString),
        Seq(s"Salted (N=$SaltBuckets)", saltedCount.toString, saltedMs.toString,
          ExperimentUtils.countExchanges(saltedPlan).toString)
      )
    )

    println(
      """
        |KEY TAKEAWAYS:
        |  • The naive join suffers from partition skew — a few tasks process most data.
        |  • Salting distributes hot-key rows across N partitions at the cost of
        |    replicating the dimension table N times (5M × 10 = 50M account rows).
        |  • The trade-off is worthwhile when the dimension table is small relative
        |    to the fact table.
        |  • In Spark UI → Stages tab, compare the max/median task duration.
        |  • AQE (Experiment 05) can also handle skew automatically — compare both.
        |""".stripMargin)

    spark.stop()
  }
}

