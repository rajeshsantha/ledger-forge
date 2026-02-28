package com.ledgerforge.pipeline.transformer

import com.ledgerforge.pipeline.SparkTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class AccountTransformerSpec extends SparkTestBase {

  private def makeAccountDF(rows: Seq[Row]) = {
    val schema = StructType(Seq(
      StructField("account_id",   IntegerType, nullable = false),
      StructField("customer_id",  IntegerType, nullable = false),
      StructField("branch_id",    IntegerType, nullable = false),
      StructField("account_type", StringType,  nullable = true),
      StructField("balance",      DoubleType,  nullable = true)
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  private val transformer = new AccountTransformer()

  // ── 1. balance_tier classification ───────────────────────────────────────
  test("balance_tier is Overdrawn for negative balance") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Checking", -500.0)))
    val tier = transformer.transform(df).select("balance_tier").first().getString(0)
    assert(tier === "Overdrawn")
  }

  test("balance_tier is Low for balance between 0 and 999") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Checking", 500.0)))
    val tier = transformer.transform(df).select("balance_tier").first().getString(0)
    assert(tier === "Low")
  }

  test("balance_tier is Medium for balance between 1000 and 9999") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Savings", 5000.0)))
    val tier = transformer.transform(df).select("balance_tier").first().getString(0)
    assert(tier === "Medium")
  }

  test("balance_tier is High for balance >= 10000") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Savings", 15000.0)))
    val tier = transformer.transform(df).select("balance_tier").first().getString(0)
    assert(tier === "High")
  }

  // ── 2. is_overdrawn flag ─────────────────────────────────────────────────
  test("is_overdrawn is true for negative balance") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Loan", -100.0)))
    val flag = transformer.transform(df).select("is_overdrawn").first().getBoolean(0)
    assert(flag === true)
  }

  test("is_overdrawn is false for zero balance") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Checking", 0.0)))
    val flag = transformer.transform(df).select("is_overdrawn").first().getBoolean(0)
    assert(flag === false)
  }

  // ── 3. account_type normalisation ────────────────────────────────────────
  test("account_type is converted to UPPER CASE") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "checking", 100.0)))
    val t = transformer.transform(df).select("account_type").first().getString(0)
    assert(t === "CHECKING")
  }

  test("account_type is trimmed of whitespace") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "  Savings  ", 100.0)))
    val t = transformer.transform(df).select("account_type").first().getString(0)
    assert(t === "SAVINGS")
  }

  // ── 4. is_credit_account ─────────────────────────────────────────────────
  test("is_credit_account is true for CREDIT type") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Credit", 100.0)))
    val flag = transformer.transform(df).select("is_credit_account").first().getBoolean(0)
    assert(flag === true)
  }

  test("is_credit_account is true for LOAN type") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Loan", 100.0)))
    val flag = transformer.transform(df).select("is_credit_account").first().getBoolean(0)
    assert(flag === true)
  }

  test("is_credit_account is false for CHECKING type") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Checking", 100.0)))
    val flag = transformer.transform(df).select("is_credit_account").first().getBoolean(0)
    assert(flag === false)
  }

  // ── 5. boundary: balance exactly at tier boundary ─────────────────────────
  test("balance_tier boundary: exactly 1000 is Medium not Low") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Savings", 1000.0)))
    val tier = transformer.transform(df).select("balance_tier").first().getString(0)
    assert(tier === "Medium")
  }

  test("balance_tier boundary: exactly 10000 is High not Medium") {
    val df = makeAccountDF(Seq(Row(1, 1, 1, "Savings", 10000.0)))
    val tier = transformer.transform(df).select("balance_tier").first().getString(0)
    assert(tier === "High")
  }
}

