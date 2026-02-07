package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class TransactionTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("processed_timestamp", current_timestamp())
  }
}
