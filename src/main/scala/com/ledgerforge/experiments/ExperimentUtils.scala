package com.ledgerforge.experiments

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Shared utilities for all Spark optimization experiments.
 *
 * Provides:
 *  - Consistent SparkSession creation with configurable settings
 *  - Standard data paths for synthetic banking datasets
 *  - Helpers for printing section headers, comparison tables, and plan analysis
 */
object ExperimentUtils {

  // ---------------------------------------------------------------------------
  // Data paths (relative to project root; matches DataGenerator output)
  // ---------------------------------------------------------------------------
  val TransactionPath = "data/synthetic/transaction"
  val AccountPath     = "data/synthetic/account"
  val CustomerPath    = "data/synthetic/customer_scd2"
  val ProductPath     = "data/synthetic/product"
  val BranchPath      = "data/synthetic/branch"

  // ---------------------------------------------------------------------------
  // Default sample fraction for quick local iteration (set to 1.0 for full runs)
  // ---------------------------------------------------------------------------
  val DefaultSampleFraction: Double = 0.01

  // ---------------------------------------------------------------------------
  // SparkSession helpers
  // ---------------------------------------------------------------------------
  def createSpark(appName: String, extraConf: Map[String, String] = Map.empty): SparkSession = {
    val builder = SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.ui.enabled", "true")
      .config("spark.ui.bindAddress", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "200")

    extraConf.foreach { case (k, v) => builder.config(k, v) }
    builder.getOrCreate()
  }

  /** Stop any active SparkSession so the next experiment can reconfigure. */
  def resetSpark(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
  }

  // ---------------------------------------------------------------------------
  // Display helpers
  // ---------------------------------------------------------------------------
  def printHeader(title: String): Unit = {
    val line = "=" * 80
    println(s"\n$line")
    println(s"  $title")
    println(s"$line\n")
  }

  def printSubHeader(title: String): Unit = {
    println(s"\n--- $title ---\n")
  }

  def printComparisonTable(header: Seq[String], rows: Seq[Seq[String]]): Unit = {
    val widths = header.indices.map { i =>
      (header(i).length +: rows.map(r => r(i).length)).max + 2
    }
    def fmtRow(r: Seq[String]): String =
      r.zip(widths).map { case (s, w) => s.padTo(w, ' ') }.mkString("| ", " | ", " |")

    val sep = widths.map("-" * _).mkString("+-", "-+-", "-+")
    println(sep)
    println(fmtRow(header))
    println(sep)
    rows.foreach(r => println(fmtRow(r)))
    println(sep)
  }

  /** Count the number of Exchange (shuffle) nodes in a physical plan string. */
  def countExchanges(plan: String): Int = {
    plan.split("\n").count(line => line.contains("Exchange") || line.contains("ShuffleExchange"))
  }

  /** Capture explain output as a String instead of printing to stdout. */
  def captureExplain(df: DataFrame, extended: Boolean = true): String = {
    val out = new java.io.ByteArrayOutputStream()
    Console.withOut(out) {
      df.explain(extended)
    }
    out.toString("UTF-8")
  }

  /** Pretty print the number of partitions of a DataFrame. */
  def printPartitionInfo(label: String, df: DataFrame): Unit = {
    println(s"$label — partitions: ${df.rdd.getNumPartitions}")
  }
}

