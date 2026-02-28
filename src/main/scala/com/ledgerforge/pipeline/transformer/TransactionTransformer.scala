package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class TransactionTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df
      // Parse transaction_date string to a proper date (CSV has format yyyy-M-d)
      .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-M-d"))

      // Classify the amount into a direction: credit vs debit
      .withColumn("amount_direction",
        when(col("amount") >= 0, lit("CREDIT"))
        .otherwise(lit("DEBIT")))

      // Absolute amount — useful for aggregations regardless of direction
      .withColumn("abs_amount", abs(col("amount")))

      // Flag suspiciously large transactions (> $10,000 — common AML threshold)
      .withColumn("is_large_transaction", col("abs_amount") > 10000)

      // Flag data quality issues
      .withColumn("is_valid",
        col("transaction_id").isNotNull &&
        col("account_id").isNotNull &&
        col("amount").isNotNull)

      .withColumn("processed_timestamp", current_timestamp())
  }
}
