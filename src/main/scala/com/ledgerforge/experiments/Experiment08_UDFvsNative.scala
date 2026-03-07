package com.ledgerforge.experiments

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

/**
 * ============================================================================
 * Experiment 08 — UDF vs Native Expressions
 * ============================================================================
 *
 * CONCEPT:
 *   Spark's Catalyst optimizer can only optimise native column expressions
 *   (when/otherwise, built-in functions).  User Defined Functions (UDFs) are
 *   opaque black boxes — Catalyst cannot:
 *     • Push them down to the data source
 *     • Combine them with adjacent operations
 *     • Generate optimised Java bytecode (WholeStageCodegen)
 *
 *   This experiment benchmarks the same logic implemented two ways:
 *     1. As a Scala UDF
 *     2. As native `when/otherwise` column expressions
 *
 * RISK BUCKET LOGIC:
 *   | Condition                               | Bucket   |
 *   |----------------------------------------|----------|
 *   | amount IS NULL or type IS NULL          | UNKNOWN  |
 *   | amount > 5000                           | CRITICAL |
 *   | amount > 1000 AND type = "Transfer"     | HIGH     |
 *   | amount > 500                            | MEDIUM   |
 *   | else                                    | LOW      |
 *
 * WHAT TO OBSERVE IN SPARK UI:
 *   • SQL tab → native version shows WholeStageCodegen(*) wrapping the
 *     entire pipeline.  UDF version breaks codegen at the UDF boundary.
 *   • Stages tab → UDF version has higher task durations due to:
 *     - Serialisation between JVM ↔ Catalyst internal row format
 *     - No vectorised execution
 *     - No predicate pushdown past the UDF
 *
 * DAG CHANGES:
 *   Both have the same logical DAG, but the physical plan differs:
 *   Native: Scan → Project (with codegen) → Action
 *   UDF:    Scan → Project (UDF breaks codegen boundary) → Action
 * ============================================================================
 */
object Experiment08_UDFvsNative {

  def main(args: Array[String]): Unit = {
    val spark = ExperimentUtils.createSpark("Exp08_UDFvsNative", Map(
      "spark.sql.adaptive.enabled" -> "false"
    ))
    import spark.implicits._

    ExperimentUtils.printHeader("Experiment 08 — UDF vs Native Expressions")

    val transactions = spark.read.parquet(ExperimentUtils.TransactionPath)

    // ========================================================================
    // Define the UDF version
    // ========================================================================
    val riskBucketUDF = udf((txnType: String, amount: java.lang.Double) => {
      if (txnType == null || amount == null) "UNKNOWN"
      else if (amount > 5000) "CRITICAL"
      else if (amount > 1000 && txnType == "Transfer") "HIGH"
      else if (amount > 500) "MEDIUM"
      else "LOW"
    })

    // ========================================================================
    // Define the native version
    // ========================================================================
    def nativeRiskBucket = {
      when(col("transaction_type").isNull || col("amount").isNull, lit("UNKNOWN"))
        .when(col("amount") > 5000, lit("CRITICAL"))
        .when(col("amount") > 1000 && col("transaction_type") === "Transfer", lit("HIGH"))
        .when(col("amount") > 500, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    }

    // ========================================================================
    // Approach 1: UDF
    // ========================================================================
    ExperimentUtils.printSubHeader("Approach 1 — Scala UDF")

    val udfDf = transactions.withColumn("risk_bucket",
      riskBucketUDF(col("transaction_type"), col("amount")))

    val udfPlan = ExperimentUtils.captureExplain(udfDf)
    println(udfPlan)

    val hasCodegenUDF = udfPlan.contains("WholeStageCodegen")
    println(s"WholeStageCodegen present? $hasCodegenUDF")

    val (udfCount, udfMs) = {
      val start = System.nanoTime()
      // Force full evaluation: groupBy risk_bucket and count
      val result = udfDf.groupBy("risk_bucket").count().collect()
      val ms = (System.nanoTime() - start) / 1000000
      result.foreach(r => println(s"  ${r.getString(0)} → ${r.getLong(1)}"))
      (result.map(_.getLong(1)).sum, ms)
    }
    println(s"UDF total rows : $udfCount, Time: ${udfMs} ms")

    // ========================================================================
    // Approach 2: Native column expressions
    // ========================================================================
    ExperimentUtils.printSubHeader("Approach 2 — Native when/otherwise")

    val nativeDf = transactions.withColumn("risk_bucket", nativeRiskBucket)

    val nativePlan = ExperimentUtils.captureExplain(nativeDf)
    println(nativePlan)

    val hasCodegenNative = nativePlan.contains("WholeStageCodegen")
    println(s"WholeStageCodegen present? $hasCodegenNative")

    val (nativeCount, nativeMs) = {
      val start = System.nanoTime()
      val result = nativeDf.groupBy("risk_bucket").count().collect()
      val ms = (System.nanoTime() - start) / 1000000
      result.foreach(r => println(s"  ${r.getString(0)} → ${r.getLong(1)}"))
      (result.map(_.getLong(1)).sum, ms)
    }
    println(s"Native total rows : $nativeCount, Time: ${nativeMs} ms")

    // ========================================================================
    // Correctness check: both should produce the same distribution
    // ========================================================================
    ExperimentUtils.printSubHeader("Correctness Check")

    val udfDistribution = udfDf.groupBy("risk_bucket").count()
      .orderBy("risk_bucket").collect().map(r => r.getString(0) -> r.getLong(1)).toMap

    val nativeDistribution = nativeDf.groupBy("risk_bucket").count()
      .orderBy("risk_bucket").collect().map(r => r.getString(0) -> r.getLong(1)).toMap

    val match_ = udfDistribution == nativeDistribution
    println(s"Distributions match? $match_")
    if (!match_) {
      println(s"UDF:    $udfDistribution")
      println(s"Native: $nativeDistribution")
    }

    // ========================================================================
    // Comparison summary
    // ========================================================================
    ExperimentUtils.printSubHeader("Comparison Summary")
    ExperimentUtils.printComparisonTable(
      Seq("Approach", "Time (ms)", "WholeStageCodegen?", "Catalyst Optimizable?"),
      Seq(
        Seq("Scala UDF", udfMs.toString, if (hasCodegenUDF) "Partial" else "No", "No"),
        Seq("Native when/otherwise", nativeMs.toString, if (hasCodegenNative) "Yes" else "No", "Yes")
      )
    )

    println(
      """
        |KEY TAKEAWAYS:
        |  • Native expressions (when/otherwise) are 2-10× faster than UDFs because:
        |    1. WholeStageCodegen generates optimised Java bytecode for the entire
        |       pipeline — UDFs break this codegen boundary.
        |    2. UDFs require deserialising each row from Catalyst InternalRow to
        |       JVM objects, calling the function, then serialising back.
        |    3. Catalyst can push predicates past native expressions but not UDFs.
        |
        |  • UDFs have valid use cases:
        |    - Complex logic that can't be expressed with built-in functions
        |    - Calling external libraries (e.g., geo-parsing, NLP)
        |    - Prototyping before implementing native equivalents
        |
        |  • If you must use a UDF, consider:
        |    - Pandas UDFs (vectorised) → much faster than row-at-a-time Scala UDFs
        |    - Registering the UDF as a SQL function for reuse
        |
        |  • RULE: Always try native expressions first.  Only resort to UDFs when
        |    the logic genuinely cannot be expressed with Spark's built-in functions.
        |
        |  • In Spark UI → SQL tab → look for "WholeStageCodegen" node.  If it's
        |    broken into multiple codegen blocks, something (likely a UDF) is
        |    preventing end-to-end code generation.
        |""".stripMargin)

    spark.stop()
  }
}

