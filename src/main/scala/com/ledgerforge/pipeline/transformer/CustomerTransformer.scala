package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class CustomerTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
      .withColumn("ingest_date", current_date())
  }
}