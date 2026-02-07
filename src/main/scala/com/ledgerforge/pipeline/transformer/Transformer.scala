package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame

trait Transformer {
  def transform(df: DataFrame): DataFrame
}