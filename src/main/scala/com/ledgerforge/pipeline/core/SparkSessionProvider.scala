package com.ledgerforge.pipeline.core

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder().master("local[*]")
      .config("spark.ui.enabled", "true")
      .config("spark.sql.shuffle.partitions", "200")
      .appName(appName).getOrCreate()
  }
}
