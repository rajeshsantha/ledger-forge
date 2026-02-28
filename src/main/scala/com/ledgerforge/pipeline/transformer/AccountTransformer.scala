package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class AccountTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df
      // Categorise balance into a human-readable tier
      .withColumn("balance_tier",
        when(col("balance") < 0,              lit("Overdrawn"))
        .when(col("balance") < 1000,          lit("Low"))
        .when(col("balance") < 10000,         lit("Medium"))
        .otherwise(                            lit("High")))

      // Flag negative balances (overdrawn accounts)
      .withColumn("is_overdrawn", col("balance") < 0)

      // Normalise account_type to upper-case for consistent downstream joins
      .withColumn("account_type", upper(trim(col("account_type"))))

      // Derived: is this a credit-bearing account?
      .withColumn("is_credit_account",
        col("account_type").isin("CREDIT", "LOAN"))

      // Static status column (existing pipeline expectation kept)
      .withColumn("account_status", lit("active"))

      .withColumn("ingest_date", current_date())
  }
}
