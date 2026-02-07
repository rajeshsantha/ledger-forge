package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame

class ProductTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumnRenamed("prod_name", "product_name")
  }
}
