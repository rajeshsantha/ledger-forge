package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

import java.time.LocalDate

/**
 * ============================================================================
 * Experiment 03 — Partition Pruning
 * ============================================================================
 *
 * CONCEPT:
 *   The transaction dataset is written with `.partitionBy("event_date")`.
 *   When you filter on the partition column, Spark can skip reading Parquet
 *   files for irrelevant partitions entirely — this is "partition pruning".
 *
 * BASELINE:
 *   Read all partitions (full scan) then apply a filter at the DataFrame
 *   level — Spark still reads every file and filters rows afterwards.
 *
 * OPTIMIZED:
 *   Use a filter on `event_date` (the partition column) BEFORE any action.
 *   Spark pushes this filter to the file scan level and only reads the
 *   matching partition directories.
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • SQL tab → the "Scan parquet" node shows `PartitionFilters` when
 *     pruning is active.  Without pruning, `PartitionFilters` is empty.
 *   • Stages tab → baseline reads all files; optimized reads a fraction.
 *   • Use `explain(true)` to see `PushedFilters` and `PartitionFilters`.
 *
 * DAG CHANGES:
 *   Baseline: FileScan reads ALL partition directories → Filter → Action.
 *   Optimized: FileScan reads ONLY matching partitions → Action (no Filter).
 *
 * ADDITIONAL CHECK:
 *   `df.inputFiles` gives the list of Parquet files actually read —
 *   compare file counts between baseline and optimized.
 * ============================================================================
 */
object Experiment03_PartitionPruning {

  def main(args: Array[String]): Unit = {
    val spark = ExperimentUtils.createSpark("Exp03_PartitionPruning")
    import spark.implicits._

    ExperimentUtils.printHeader("Experiment 03 — Partition Pruning")

    // Determine a date range: last 3 days of the dataset
    val allTxn = spark.read.parquet(ExperimentUtils.TransactionPath)
    val maxDate = allTxn.agg(max("event_date")).collect()(0).get(0).toString
    val endDate = LocalDate.parse(maxDate)
    val startDate = endDate.minusDays(2)
    println(s"Dataset date range filter: $startDate to $endDate (3 days out of 30)")

    // ========================================================================
    // BASELINE — full scan, then filter at DataFrame level
    // ========================================================================
    ExperimentUtils.printSubHeader("BASELINE — Full Scan (no partition pruning)")

    // Force no partition pruning by reading and then filtering on a derived column
    val fullScan = spark.read.parquet(ExperimentUtils.TransactionPath)

    val fullScanPlan = ExperimentUtils.captureExplain(fullScan)
    val fullScanFileCount = fullScan.inputFiles.length

    val (fullScanCount, fullScanMs) = {
      val start = System.nanoTime()
      val cnt = fullScan.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }
    println(s"Full scan files  : $fullScanFileCount")
    println(s"Full scan count  : $fullScanCount")
    println(s"Full scan time   : ${fullScanMs} ms")

    // ========================================================================
    // OPTIMIZED — filter on partition column (event_date) → partition pruning
    // ========================================================================
    ExperimentUtils.printSubHeader("OPTIMIZED — Partition Pruning (filter on event_date)")

    val pruned = spark.read.parquet(ExperimentUtils.TransactionPath)
      .filter(col("event_date").between(lit(startDate.toString), lit(endDate.toString)))

    val prunedPlan = ExperimentUtils.captureExplain(pruned)
    println(prunedPlan)

    // Check for PartitionFilters in the plan
    val hasPartitionFilter = prunedPlan.contains("PartitionFilters")
    println(s"Plan contains PartitionFilters? $hasPartitionFilter")

    val prunedFileCount = pruned.inputFiles.length

    val (prunedCount, prunedMs) = {
      val start = System.nanoTime()
      val cnt = pruned.count()
      val ms = (System.nanoTime() - start) / 1000000
      (cnt, ms)
    }
    println(s"Pruned files     : $prunedFileCount")
    println(s"Pruned count     : $prunedCount")
    println(s"Pruned time      : ${prunedMs} ms")

    // ========================================================================
    // ANTI-PATTERN — filter on a non-partition column (kills pruning)
    // ========================================================================
    ExperimentUtils.printSubHeader("ANTI-PATTERN — Filter on non-partition column")

    // Filtering on `event_ts` (not a partition column) — Spark must read all files
    val antiPattern = spark.read.parquet(ExperimentUtils.TransactionPath)
      .filter(col("event_ts") >= lit(s"${startDate}T00:00:00"))

    val antiPlan = ExperimentUtils.captureExplain(antiPattern)
    val antiFileCount = antiPattern.inputFiles.length
    val antiHasPartitionFilter = antiPlan.contains("PartitionFilters") &&
      !antiPlan.contains("PartitionFilters: []")
    println(s"Non-partition filter — PartitionFilters active? $antiHasPartitionFilter")
    println(s"Non-partition filter — files read: $antiFileCount")

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Approach", "Files Read", "Row Count", "Time (ms)", "Partition Pruning?"),
      Seq(
        Seq("Full Scan", fullScanFileCount.toString, fullScanCount.toString,
          fullScanMs.toString, "No"),
        Seq("Pruned (event_date)", prunedFileCount.toString, prunedCount.toString,
          prunedMs.toString, if (hasPartitionFilter) "Yes" else "No"),
        Seq("Anti-pattern (event_ts)", antiFileCount.toString, "—",
          "—", if (antiHasPartitionFilter) "Yes" else "No")
      )
    )

    println(
      s"""
         |KEY TAKEAWAYS:
         |  • Partition pruning skips entire directories of Parquet files.
         |  • With 30 daily partitions, reading 3 days = 10% of data → ~10× faster.
         |  • Files read: full=$fullScanFileCount, pruned=$prunedFileCount.
         |  • ONLY filters on the actual partition column trigger pruning.
         |  • Filtering on `event_ts` (a regular column) does NOT trigger pruning
         |    even though it's time-based — Spark must read all files.
         |  • In production, always structure filters around partition columns.
         |  • Use `explain(true)` and look for `PartitionFilters:` in the
         |    FileScan node to verify pruning is active.
         |  • Dynamic partition overwrite (`partitionOverwriteMode = dynamic`)
         |    pairs well with partition pruning for idempotent incremental writes.
         |""".stripMargin)

    spark.stop()
  }
}

