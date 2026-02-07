package com.ledgerforge.pipeline.reader

import com.ledgerforge.pipeline.schema.SchemaProvider
import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVReader(spark: SparkSession) {
  def read(path: String): DataFrame = {
    val schema = SchemaProvider.schemaForPath(path)
    spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(schema)
      .csv(path)
  }
}
