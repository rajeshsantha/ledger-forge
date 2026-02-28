package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ProductTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df
      // Rename to consistent column name used everywhere else
      .withColumnRenamed("prod_name", "product_name")

      // Infer product category from the name
      // CSV data has names like "Debit Card 1", "Credit Card 2", "Savings 3"
      .withColumn("product_category",
        when(lower(col("product_name")).contains("debit"),   lit("Debit"))
        .when(lower(col("product_name")).contains("credit"), lit("Credit"))
        .when(lower(col("product_name")).contains("saving"), lit("Savings"))
        .when(lower(col("product_name")).contains("loan"),   lit("Loan"))
        .when(lower(col("product_name")).contains("check"),  lit("Checking"))
        .otherwise(lit("Other")))

      // Flag whether the product is a lending product (bank takes risk)
      .withColumn("is_lending_product",
        col("product_category").isin("Credit", "Loan"))

      // Normalise product_name — strip trailing numbers ("Debit Card 1" → "Debit Card")
      .withColumn("product_name_clean",
        trim(regexp_replace(col("product_name"), "\\s+\\d+$", "")))

      .withColumn("ingest_date", current_date())
  }
}
