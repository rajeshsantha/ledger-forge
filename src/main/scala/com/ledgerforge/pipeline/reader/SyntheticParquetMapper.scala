package com.ledgerforge.pipeline.reader

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType}

object SyntheticParquetMapper {
  def mapToCsvSchema(df: DataFrame, pathHint: String): DataFrame = {
    val p = pathHint.toLowerCase
    if (p.contains("transaction")) mapTransaction(df)
    else if (p.contains("customer_scd2") || p.contains("customer")) mapCustomer(df)
    else if (p.contains("account")) mapAccount(df)
    else if (p.contains("product")) mapProduct(df)
    else if (p.contains("branch")) mapBranch(df)
    else throw new IllegalArgumentException(s"Unknown synthetic dataset for path: $pathHint")
  }

  private def mapTransaction(df: DataFrame): DataFrame = {
    df.select(
      col("transaction_id"),
      col("account_id"),
      col("product_id"),
      col("amount"),
      col("event_date").cast(StringType).as("transaction_date")
    )
  }

  private def mapCustomer(df: DataFrame): DataFrame = {
    df.select(
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("email"),
      col("phone")
    )
  }

  private def mapAccount(df: DataFrame): DataFrame = {
    df.select(
      col("account_id"),
      col("customer_id"),
      col("branch_id"),
      col("account_type"),
      col("balance")
    )
  }

  private def mapProduct(df: DataFrame): DataFrame = {
    df.select(
      col("product_id"),
      col("product_name").as("prod_name"),
      col("product_group").as("description")
    )
  }

  private def mapBranch(df: DataFrame): DataFrame = {
    df.select(
      col("branch_id"),
      col("branch_name"),
      lit(null).cast(StringType).as("city"),
      col("state")
    )
  }
}
