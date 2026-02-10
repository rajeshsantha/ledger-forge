package com.ledgerforge.pipeline.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetReader(spark: SparkSession) {
  def read(path: String): DataFrame = {
    val df = spark.read.parquet(path)
    SyntheticParquetMapper.mapToCsvSchema(df, path)
  }
}
