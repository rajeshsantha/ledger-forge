package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * ============================================================================
 * Experiment 06 — Deduplication Strategies
 * ============================================================================
 *
 * CONCEPT:
 *   The transaction dataset has ~2% duplicate `transaction_id` values with
 *   different `ingest_date`.  We need to keep the LATEST version (by
 *   `ingest_date`, then `event_ts`).
 *
 *   Different dedup approaches have different correctness and performance
 *   characteristics.
 *
 * APPROACHES COMPARED:
 *   A) dropDuplicates("transaction_id")
 *      — Keeps an ARBITRARY row (not necessarily the latest).
 *      — Fastest, but INCORRECT if you need deterministic "latest wins".
 *
 *   B) Window row_number()
 *      — partitionBy("transaction_id").orderBy(ingest_date.desc, event_ts.desc)
 *      — Correct: keeps the latest row. But requires a full sort per partition.
 *      — Causes a shuffle + sort.
 *
 *   C) groupBy + max_by (Spark 3.4+) / struct trick
 *      — groupBy("transaction_id").agg(max_by(struct("*"), struct(desc("ingest_date"))))
 *      — Correct and potentially faster than window (no sort, just aggregate).
 *      — Note: `max_by` is available in Spark 3.4+.
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • Approach A: 1 Exchange (hash on transaction_id) + HashAggregate.
 *   • Approach B: 1 Exchange + Sort + Window.  Sort is expensive on 50M rows.
 *   • Approach C: 1 Exchange + HashAggregate.  No sort step.
 *   • Compare: task duration and total time.
 *
 * CORRECTNESS CHECK:
 *   For a known duplicate transaction_id, verify which row was kept.
 * ============================================================================
 */
object Experiment06_DeduplicationStrategies {

  def main(args: Array[String]): Unit = {
    val spark = ExperimentUtils.createSpark("Exp06_Dedup", Map(
      "spark.sql.adaptive.enabled" -> "false"  // fair comparison
    ))
    import spark.implicits._

    ExperimentUtils.printHeader("Experiment 06 — Deduplication Strategies")

    val raw = spark.read.parquet(ExperimentUtils.TransactionPath)
    val totalRows = raw.count()
    val distinctTxnIds = raw.select("transaction_id").distinct().count()
    val duplicates = totalRows - distinctTxnIds
    println(s"Total rows         : $totalRows")
    println(s"Distinct txn IDs   : $distinctTxnIds")
    println(s"Duplicate rows     : $duplicates (~${(duplicates * 100.0 / totalRows).formatted("%.2f")}%)")

    // Find a sample duplicate for correctness verification
    val sampleDupId = raw.groupBy("transaction_id").count()
      .filter(col("count") > 1)
      .select("transaction_id")
      .head()
      .getLong(0)
    println(s"Sample duplicate transaction_id: $sampleDupId")
    println("All versions of this transaction:")
    raw.filter(col("transaction_id") === sampleDupId)
      .select("transaction_id", "ingest_date", "event_ts", "amount")
      .orderBy(desc("ingest_date"), desc("event_ts"))
      .show(truncate = false)

    // ========================================================================
    // Approach A: dropDuplicates (fast but non-deterministic)
    // ========================================================================
    ExperimentUtils.printSubHeader("Approach A — dropDuplicates (non-deterministic)")

    val (countA, msA) = {
      val start = System.nanoTime()
      val deduped = raw.dropDuplicates("transaction_id")
      val cnt = deduped.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }
    println(s"Count after dedup: $countA, Time: ${msA} ms")

    // Check which row was kept for sample dup
    val keptA = raw.dropDuplicates("transaction_id")
      .filter(col("transaction_id") === sampleDupId)
      .select("transaction_id", "ingest_date", "event_ts")
    println("Kept row (arbitrary):")
    keptA.show(truncate = false)

    val planA = ExperimentUtils.captureExplain(raw.dropDuplicates("transaction_id"))
    println(s"Exchanges: ${ExperimentUtils.countExchanges(planA)}")

    // ========================================================================
    // Approach B: Window row_number (correct + deterministic)
    // ========================================================================
    ExperimentUtils.printSubHeader("Approach B — Window row_number (deterministic)")

    val w = Window.partitionBy("transaction_id")
      .orderBy(col("ingest_date").desc, col("event_ts").desc)

    val (countB, msB) = {
      val start = System.nanoTime()
      val deduped = raw
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") === 1)
        .drop("rn")
      val cnt = deduped.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }
    println(s"Count after dedup: $countB, Time: ${msB} ms")

    // Verify correct row kept
    val keptB = raw
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1 && col("transaction_id") === sampleDupId)
      .select("transaction_id", "ingest_date", "event_ts")
    println("Kept row (latest by ingest_date, event_ts):")
    keptB.show(truncate = false)

    val planB = ExperimentUtils.captureExplain(
      raw.withColumn("rn", row_number().over(w)).filter(col("rn") === 1).drop("rn")
    )
    println(s"Exchanges: ${ExperimentUtils.countExchanges(planB)}")

    // ========================================================================
    // Approach C: groupBy + struct max trick (correct, no sort)
    // ========================================================================
    ExperimentUtils.printSubHeader("Approach C — groupBy + struct max (correct, aggregate-only)")

    // Struct trick: max(struct(ordering_cols, data_cols)) picks the row with
    // the max ordering columns.  We negate ingest_date by using datediff trick.
    val (countC, msC) = {
      val start = System.nanoTime()
      val allCols = raw.columns.filter(_ != "transaction_id")

      // Build a struct with ordering columns first, then all data columns
      val orderStruct = struct(
        col("ingest_date"),
        col("event_ts"),
        struct(allCols.map(col): _*).as("data")
      )

      val deduped = raw
        .groupBy("transaction_id")
        .agg(max(orderStruct).as("latest"))
        .select(
          col("transaction_id") +:
          allCols.map(c => col(s"latest.data.$c").as(c)): _*
        )
      val cnt = deduped.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }
    println(s"Count after dedup: $countC, Time: ${msC} ms")

    val planC = {
      val allCols = raw.columns.filter(_ != "transaction_id")
      val orderStruct = struct(col("ingest_date"), col("event_ts"),
        struct(allCols.map(col): _*).as("data"))
      ExperimentUtils.captureExplain(
        raw.groupBy("transaction_id").agg(max(orderStruct).as("latest"))
      )
    }
    println(s"Exchanges: ${ExperimentUtils.countExchanges(planC)}")

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Approach", "Count", "Time (ms)", "Correct?", "Exchanges", "Sort?"),
      Seq(
        Seq("dropDuplicates", countA.toString, msA.toString, "No (arbitrary)", "1", "No"),
        Seq("Window row_number", countB.toString, msB.toString, "Yes (latest)", "1", "Yes"),
        Seq("groupBy + struct max", countC.toString, msC.toString, "Yes (latest)", "1", "No")
      )
    )

    println(
      """
        |KEY TAKEAWAYS:
        |  • dropDuplicates is fast but non-deterministic — it keeps an arbitrary
        |    row, NOT necessarily the latest.  NEVER use it if "latest wins" matters.
        |  • Window row_number is the standard pattern for deterministic dedup.
        |    It requires a sort (expensive on 50M rows) but gives full control
        |    over which row to keep.
        |  • The struct-max trick (groupBy + max(struct(...))) avoids the sort step
        |    by leveraging Spark's aggregate engine.  It uses struct comparison:
        |    struct("2024-01-03", ...) > struct("2024-01-01", ...) → keeps latest.
        |  • All three approaches require 1 Exchange (shuffle by transaction_id).
        |    The difference is whether there's an additional Sort step.
        |  • In Spark UI → Stages tab, look for the Sort stage in Approach B
        |    that's absent in A and C.
        |""".stripMargin)

    spark.stop()
  }
}

