package com.ledgerforge.pipeline.transformer

import com.ledgerforge.pipeline.SparkTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class BranchTransformerSpec extends SparkTestBase {

  private def makeBranchDF(rows: Seq[Row]) = {
    val schema = StructType(Seq(
      StructField("branch_id",   IntegerType, nullable = false),
      StructField("branch_name", StringType,  nullable = true),
      StructField("city",        StringType,  nullable = true),
      StructField("state",       StringType,  nullable = true)
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  private val transformer = new BranchTransformer()

  // ── 1. branch_upper ───────────────────────────────────────────────────────
  test("branch_upper is the branch_name in upper case") {
    val df = makeBranchDF(Seq(Row(1, "Main Branch 1", "City 1", "NY")))
    val upper = transformer.transform(df).select("branch_upper").first().getString(0)
    assert(upper === "MAIN BRANCH 1")
  }

  // ── 2. city/state whitespace trimming ─────────────────────────────────────
  // This is a REAL DATA BUG in branch.csv: " State 2" has a leading space
  test("city is trimmed of leading and trailing whitespace") {
    val df = makeBranchDF(Seq(Row(1, "Main Branch 1", "  City 70  ", "NY")))
    val city = transformer.transform(df).select("city").first().getString(0)
    assert(city === "City 70")
  }

  test("state is trimmed of leading whitespace (actual CSV data bug)") {
    val df = makeBranchDF(Seq(Row(1, "Main Branch 1", "City 1", " State 2")))
    val state = transformer.transform(df).select("state").first().getString(0)
    assert(state === "State 2")
  }

  // ── 3. branch_type extraction ─────────────────────────────────────────────
  test("branch_type is Main for names containing 'main'") {
    val df = makeBranchDF(Seq(Row(1, "Main Branch 1", "City", "NY")))
    val t = transformer.transform(df).select("branch_type").first().getString(0)
    assert(t === "Main")
  }

  test("branch_type is Downtown for names containing 'downtown'") {
    val df = makeBranchDF(Seq(Row(1, "Downtown Branch 2", "City", "CA")))
    val t = transformer.transform(df).select("branch_type").first().getString(0)
    assert(t === "Downtown")
  }

  test("branch_type is Online for names containing 'online'") {
    val df = makeBranchDF(Seq(Row(1, "Online Branch", "N/A", "N/A")))
    val t = transformer.transform(df).select("branch_type").first().getString(0)
    assert(t === "Online")
  }

  test("branch_type is Other for unrecognised branch names") {
    val df = makeBranchDF(Seq(Row(1, "Mystery Branch", "City", "TX")))
    val t = transformer.transform(df).select("branch_type").first().getString(0)
    assert(t === "Other")
  }

  // ── 4. is_state_valid ─────────────────────────────────────────────────────
  test("is_state_valid is true for a standard 2-letter state code") {
    val df = makeBranchDF(Seq(Row(1, "Branch", "City", "NY")))
    val valid = transformer.transform(df).select("is_state_valid").first().getBoolean(0)
    assert(valid === true)
  }

  test("is_state_valid is false for a value like 'State 2' (from raw CSV)") {
    // After trimming, "State 2" is still not a 2-letter code
    val df = makeBranchDF(Seq(Row(1, "Branch", "City", " State 2")))
    val valid = transformer.transform(df).select("is_state_valid").first().getBoolean(0)
    assert(valid === false)
  }

  test("is_state_valid is false for null state") {
    val df = makeBranchDF(Seq(Row(1, "Branch", "City", null)))
    val valid = transformer.transform(df).select("is_state_valid").first().getBoolean(0)
    assert(valid === false)
  }

  test("is_state_valid is false for lowercase state code") {
    // Our regex requires uppercase: ^[A-Z]{2}$
    val df = makeBranchDF(Seq(Row(1, "Branch", "City", "ny")))
    val valid = transformer.transform(df).select("is_state_valid").first().getBoolean(0)
    assert(valid === false)
  }
}

