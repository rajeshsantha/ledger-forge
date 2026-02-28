package com.ledgerforge.pipeline.writer

import org.apache.spark.sql.DataFrame

class ParquetWriter(outputBasePath: String) {
  def write(df: DataFrame, tableName: String): Unit = {
    val outputPath = s"$outputBasePath/$tableName"
    df.write
      .mode("overwrite")
      .parquet(outputPath)
  }
}

