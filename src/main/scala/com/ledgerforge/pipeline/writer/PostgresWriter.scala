// scala
package com.ledgerforge.pipeline.writer

import org.apache.spark.sql.DataFrame
import com.typesafe.config.Config

class PostgresWriter(config: Config) {
  private val dbConfig =
    if (config.hasPath("db")) config.getConfig("db")
    else throw new IllegalArgumentException("Missing 'db' configuration. Put db { url } in src/main/resources/application.conf")

  private val url = dbConfig.getString("url")
  private val user = sys.env.getOrElse("DB_USER", throw new IllegalArgumentException("Missing environment variable DB_USER"))
  private val password = sys.env.getOrElse("DB_PASSWORD", throw new IllegalArgumentException("Missing environment variable DB_PASSWORD"))
  def write(df: DataFrame, tableName: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", url)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .mode("overwrite")
      .save()
  }
}
