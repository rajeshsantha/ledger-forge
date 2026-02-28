package com.ledgerforge.pipeline.transformer

import com.ledgerforge.pipeline.SparkTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class TransactionTransformerSpec extends SparkTestBase {

  private def makeTxnDF(rows: Seq[Row]) = {
    val schema = StructType(Seq(
      StructField("transaction_id",   IntegerType, nullable = false),
      StructField("account_id",       IntegerType, nullable = false),
      StructField("product_id",       IntegerType, nullable = false),
      StructField("amount",           DoubleType,  nullable = true),
      StructField("transaction_date", StringType,  nullable = true)   // raw string from CSV
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  private val transformer = new TransactionTransformer()

  // ── 1. date parsing ──────────────────────────────────────────────────────
  test("transaction_date is parsed from non-padded string yyyy-M-d to DateType") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, 100.0, "2023-2-28")))
    val result = transformer.transform(df)

    // Column must now be of DateType, not StringType
    val dateField = result.schema("transaction_date")
    assert(dateField.dataType === org.apache.spark.sql.types.DateType,
      s"Expected DateType but got ${dateField.dataType}")

    // Value must be non-null
    assert(result.select("transaction_date").first().get(0) !== null)
  }

  test("transaction_date is null when input string is null") {
    val df = makeTxnDF(Seq(Row(2, 1, 1, 100.0, null)))
    val result = transformer.transform(df).select("transaction_date").first().get(0)
    assert(result === null)
  }

  // ── 2. amount_direction ──────────────────────────────────────────────────
  test("amount_direction is CREDIT for positive amount") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, 250.0, "2023-1-1")))
    val dir = transformer.transform(df).select("amount_direction").first().getString(0)
    assert(dir === "CREDIT")
  }

  test("amount_direction is CREDIT for zero amount") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, 0.0, "2023-1-1")))
    val dir = transformer.transform(df).select("amount_direction").first().getString(0)
    assert(dir === "CREDIT")
  }

  test("amount_direction is DEBIT for negative amount") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, -50.0, "2023-1-1")))
    val dir = transformer.transform(df).select("amount_direction").first().getString(0)
    assert(dir === "DEBIT")
  }

  // ── 3. abs_amount ────────────────────────────────────────────────────────
  test("abs_amount is positive for a negative amount") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, -75.50, "2023-1-1")))
    val abs = transformer.transform(df).select("abs_amount").first().getDouble(0)
    assert(abs === 75.50)
  }

  test("abs_amount equals amount for a positive amount") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, 200.0, "2023-1-1")))
    val abs = transformer.transform(df).select("abs_amount").first().getDouble(0)
    assert(abs === 200.0)
  }

  // ── 4. is_large_transaction (AML threshold > 10000) ──────────────────────
  test("is_large_transaction is false for amount <= 10000") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, 10000.0, "2023-1-1")))
    val flag = transformer.transform(df).select("is_large_transaction").first().getBoolean(0)
    assert(flag === false)
  }

  test("is_large_transaction is true for amount > 10000") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, 10001.0, "2023-1-1")))
    val flag = transformer.transform(df).select("is_large_transaction").first().getBoolean(0)
    assert(flag === true)
  }

  test("is_large_transaction uses absolute value — negative large amount is also flagged") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, -15000.0, "2023-1-1")))
    val flag = transformer.transform(df).select("is_large_transaction").first().getBoolean(0)
    assert(flag === true)
  }

  // ── 5. is_valid ───────────────────────────────────────────────────────────
  test("is_valid is true when transaction_id, account_id and amount are all non-null") {
    val df = makeTxnDF(Seq(Row(1, 42, 1, 100.0, "2023-1-1")))
    val valid = transformer.transform(df).select("is_valid").first().getBoolean(0)
    assert(valid === true)
  }

  test("is_valid is false when amount is null") {
    val df = makeTxnDF(Seq(Row(1, 42, 1, null, "2023-1-1")))
    val valid = transformer.transform(df).select("is_valid").first().getBoolean(0)
    assert(valid === false)
  }

  // ── 6. processed_timestamp is always present ──────────────────────────────
  test("processed_timestamp column exists and is not null") {
    val df = makeTxnDF(Seq(Row(1, 1, 1, 50.0, "2023-1-1")))
    val result = transformer.transform(df)
    assert(result.columns.contains("processed_timestamp"))
    assert(result.select("processed_timestamp").first().get(0) !== null)
  }
}

