package com.ledgerforge.pipeline.core

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  def getSparkSession(appName: String): SparkSession = {
    // Bind driver and UI to localhost to avoid Netty binding to an unresolved host
    SparkSession.builder().master("local[*]")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.bindAddress", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "200")
      .appName(appName).getOrCreate()
  }
}
