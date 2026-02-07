package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class BranchTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("branch_upper", upper(col("branch_name")))
  }
}
