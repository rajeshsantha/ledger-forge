package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class AccountTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("account_status", lit("active"))
  }
}

