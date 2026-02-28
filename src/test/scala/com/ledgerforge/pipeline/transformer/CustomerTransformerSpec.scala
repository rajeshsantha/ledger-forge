package com.ledgerforge.pipeline.transformer

import com.ledgerforge.pipeline.SparkTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CustomerTransformerSpec extends SparkTestBase {

  // ── helper: build a DataFrame from raw rows without reading any files ──────
  private def makeCustomerDF(rows: Seq[Row]) = {
    val schema = StructType(Seq(
      StructField("customer_id", IntegerType, nullable = false),
      StructField("first_name",  StringType,  nullable = true),
      StructField("last_name",   StringType,  nullable = true),
      StructField("email",       StringType,  nullable = true),
      StructField("phone",       StringType,  nullable = true)
    ))
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows), schema
    )
  }

  private val transformer = new CustomerTransformer()

  // ── 1. full_name derivation ───────────────────────────────────────────────
  test("full_name is first_name + last_name joined by a space") {
    val df = makeCustomerDF(Seq(Row(1, "john", "williams", "j@x.com", "555-123-4567")))
    val result = transformer.transform(df)

    // After initcap, "john" → "John"
    val fullName = result.select("full_name").first().getString(0)
    assert(fullName === "John Williams")
  }

  // ── 2. name standardisation ───────────────────────────────────────────────
  test("first_name and last_name are trimmed and title-cased") {
    val df = makeCustomerDF(Seq(Row(2, "  aLICE  ", "  sMITH  ", "a@x.com", "555-000-0000")))
    val result = transformer.transform(df).first()

    assert(result.getAs[String]("first_name") === "Alice")
    assert(result.getAs[String]("last_name")  === "Smith")
  }

  // ── 3. email_domain extraction ────────────────────────────────────────────
  test("email_domain extracts the part after @") {
    val df = makeCustomerDF(Seq(Row(3, "Bob", "Jones", "bob.jones@gmail.com", "555-000-0000")))
    val domain = transformer.transform(df).select("email_domain").first().getString(0)
    assert(domain === "gmail.com")
  }

  test("email_domain is null when email is null") {
    val df = makeCustomerDF(Seq(Row(4, "Eve", "Doe", null, "555-000-0000")))
    val domain = transformer.transform(df).select("email_domain").first().get(0)
    assert(domain === null)
  }

  test("email_domain is null when email has no @ sign") {
    val df = makeCustomerDF(Seq(Row(5, "Frank", "Brown", "invalid_email", "555-000-0000")))
    val domain = transformer.transform(df).select("email_domain").first().get(0)
    assert(domain === null)
  }

  // ── 4. is_email_valid ─────────────────────────────────────────────────────
  test("is_email_valid is true for a well-formed email") {
    val df = makeCustomerDF(Seq(Row(6, "A", "B", "a.b@example.com", "555-123-4567")))
    val valid = transformer.transform(df).select("is_email_valid").first().getBoolean(0)
    assert(valid === true)
  }

  test("is_email_valid is false for a malformed email") {
    val df = makeCustomerDF(Seq(Row(7, "A", "B", "invalid_email", "555-123-4567")))
    val valid = transformer.transform(df).select("is_email_valid").first().getBoolean(0)
    assert(valid === false)
  }

  test("is_email_valid is false when email is null") {
    val df = makeCustomerDF(Seq(Row(8, "A", "B", null, "555-123-4567")))
    val valid = transformer.transform(df).select("is_email_valid").first().getBoolean(0)
    assert(valid === false)
  }

  // ── 5. is_phone_valid ─────────────────────────────────────────────────────
  test("is_phone_valid is true for format 555-123-4567") {
    val df = makeCustomerDF(Seq(Row(9, "A", "B", "a@b.com", "555-123-4567")))
    val valid = transformer.transform(df).select("is_phone_valid").first().getBoolean(0)
    assert(valid === true)
  }

  test("is_phone_valid is false for a non-standard phone") {
    val df = makeCustomerDF(Seq(Row(10, "A", "B", "a@b.com", "000-000-0000")))
    // 000-000-0000 technically matches the regex — check for a truly invalid one
    val df2 = makeCustomerDF(Seq(Row(10, "A", "B", "a@b.com", "5551234567")))
    val valid = transformer.transform(df2).select("is_phone_valid").first().getBoolean(0)
    assert(valid === false)
  }

  // ── 6. ingest_date is always present ─────────────────────────────────────
  test("ingest_date column exists and is not null") {
    val df = makeCustomerDF(Seq(Row(11, "A", "B", "a@b.com", "555-123-4567")))
    val result = transformer.transform(df)
    assert(result.columns.contains("ingest_date"))
    assert(result.select("ingest_date").first().get(0) !== null)
  }

  // ── 7. schema — no original columns are dropped ──────────────────────────
  test("transform does not drop any original columns") {
    val df = makeCustomerDF(Seq(Row(12, "A", "B", "a@b.com", "555-111-2222")))
    val result = transformer.transform(df)
    val original = Seq("customer_id", "first_name", "last_name", "email", "phone")
    original.foreach(col => assert(result.columns.contains(col), s"Missing column: $col"))
  }

  // ── 8. multiple rows — transformer is row-independent ────────────────────
  test("transform handles multiple rows correctly") {
    val df = makeCustomerDF(Seq(
      Row(1, "alice", "smith",    "alice@x.com",   "555-100-0001"),
      Row(2, "bob",   "jones",    "bob@y.com",      "555-200-0002"),
      Row(3, "carol", "williams", "invalid",        null)
    ))
    val result = transformer.transform(df)
    assert(result.count() === 3)

    val names = result.select("full_name").collect().map(_.getString(0))
    assert(names.contains("Alice Smith"))
    assert(names.contains("Bob Jones"))
    assert(names.contains("Carol Williams"))
  }
}

