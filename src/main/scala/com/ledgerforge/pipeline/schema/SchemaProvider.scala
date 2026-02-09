package com.ledgerforge.pipeline.schema

import org.apache.spark.sql.types.{StructField, _}

object SchemaProvider {
  val customerSchema: StructType = StructType(Seq(
    StructField("customer_id", IntegerType, nullable = false),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("phone", StringType, nullable = true)
  ))

  val accountSchema: StructType = StructType(Seq(
    StructField("account_id", IntegerType, nullable = false),
    StructField("customer_id", IntegerType, nullable = false),
    StructField("branch_id", IntegerType, nullable = false),
    StructField("account_type", StringType, nullable = true),
    StructField("balance", DoubleType, nullable = true)
  ))

  val transactionSchema: StructType = StructType(Seq(
    StructField("transaction_id", IntegerType, nullable = false),
    StructField("account_id", IntegerType, nullable = false),
    StructField("product_id", IntegerType, nullable = false),
    StructField("amount", DoubleType, nullable = true),
    // keep date as string to avoid strict parsing; callers can parse to Date if needed
    StructField("transaction_date", StringType, nullable = true)
  ))

  val productSchema: StructType = StructType(Seq(
    StructField("product_id", IntegerType, nullable = false),
    StructField("prod_name", StringType, nullable = true),
    StructField("description", StringType, nullable = true)
  ))

  val branchSchema: StructType = StructType(Seq(
    StructField("branch_id", IntegerType, nullable = false),
    StructField("branch_name", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
      StructField("state", StringType, nullable = true)
  ))

  def schemaForPath(path: String): StructType = {
val p = path.toLowerCase
Seq(
  "customer"     -> customerSchema,
  "account"      -> accountSchema,
  "transaction"  -> transactionSchema,
  "product"      -> productSchema,
  "branch"       -> branchSchema
).collectFirst { case (k, s) if p.contains(k) => s }
 .getOrElse(throw new IllegalArgumentException(s"Unknown schema for path: $path"))  }
}
