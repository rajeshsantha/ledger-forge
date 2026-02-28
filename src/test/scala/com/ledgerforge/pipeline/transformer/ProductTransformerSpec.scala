package com.ledgerforge.pipeline.transformer

import com.ledgerforge.pipeline.SparkTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class ProductTransformerSpec extends SparkTestBase {

  private def makeProductDF(rows: Seq[Row]) = {
    val schema = StructType(Seq(
      StructField("product_id",   IntegerType, nullable = false),
      StructField("prod_name",    StringType,  nullable = true),
      StructField("description",  StringType,  nullable = true)
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  private val transformer = new ProductTransformer()

  // ── 1. rename prod_name → product_name ───────────────────────────────────
  test("prod_name column is renamed to product_name") {
    val df = makeProductDF(Seq(Row(1, "Debit Card 1", "desc")))
    val result = transformer.transform(df)
    assert(result.columns.contains("product_name"), "product_name column should exist")
    assert(!result.columns.contains("prod_name"),   "prod_name column should be gone")
  }

  // ── 2. product_category inference ────────────────────────────────────────
  test("product_category is Debit for names containing 'debit'") {
    val df = makeProductDF(Seq(Row(1, "Debit Card 1", "desc")))
    val cat = transformer.transform(df).select("product_category").first().getString(0)
    assert(cat === "Debit")
  }

  test("product_category is Credit for names containing 'credit'") {
    val df = makeProductDF(Seq(Row(1, "Credit Card 2", "desc")))
    val cat = transformer.transform(df).select("product_category").first().getString(0)
    assert(cat === "Credit")
  }

  test("product_category is Savings for names containing 'saving'") {
    val df = makeProductDF(Seq(Row(1, "Savings 3", "desc")))
    val cat = transformer.transform(df).select("product_category").first().getString(0)
    assert(cat === "Savings")
  }

  test("product_category is Other for unrecognised product names") {
    val df = makeProductDF(Seq(Row(1, "Mystery Product", "desc")))
    val cat = transformer.transform(df).select("product_category").first().getString(0)
    assert(cat === "Other")
  }

  // ── 3. is_lending_product ─────────────────────────────────────────────────
  test("is_lending_product is true for Credit category") {
    val df = makeProductDF(Seq(Row(1, "Credit Card 1", "desc")))
    val flag = transformer.transform(df).select("is_lending_product").first().getBoolean(0)
    assert(flag === true)
  }

  test("is_lending_product is false for Debit category") {
    val df = makeProductDF(Seq(Row(1, "Debit Card 1", "desc")))
    val flag = transformer.transform(df).select("is_lending_product").first().getBoolean(0)
    assert(flag === false)
  }

  // ── 4. product_name_clean strips trailing numbers ─────────────────────────
  test("product_name_clean strips trailing number from product name") {
    val df = makeProductDF(Seq(Row(1, "Debit Card 1", "desc")))
    val clean = transformer.transform(df).select("product_name_clean").first().getString(0)
    assert(clean === "Debit Card")
  }

  test("product_name_clean strips multi-digit trailing numbers") {
    val df = makeProductDF(Seq(Row(1, "Savings 42", "desc")))
    val clean = transformer.transform(df).select("product_name_clean").first().getString(0)
    assert(clean === "Savings")
  }

  test("product_name_clean leaves names without trailing numbers unchanged") {
    val df = makeProductDF(Seq(Row(1, "Mortgage", "desc")))
    val clean = transformer.transform(df).select("product_name_clean").first().getString(0)
    assert(clean === "Mortgage")
  }
}

