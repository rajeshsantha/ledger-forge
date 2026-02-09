package com.ledgerforge.pipeline.reader

import com.ledgerforge.pipeline.schema.SchemaProvider
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths}

class CSVReader(spark: SparkSession) {
  /**
   * Read a CSV file into a DataFrame using a schema inferred from the path.
   * This preserves the original strict (FAILFAST) behavior but performs an
   * explicit existence check and wraps any exception with a clearer message
   * (file existence and size) to help diagnose problems like the one reported
   * when reading `branch.csv`.
   */
  def read(path: String): DataFrame = {
    val p = Paths.get(path)
    if (!Files.exists(p)) {
      throw new java.io.FileNotFoundException(s"CSV file not found at: $path")
    }

    val schema = SchemaProvider.schemaForPath(path)

    try {
      spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(path)
    } catch {
      case e: Exception =>
        val size = try { Files.size(p).toString } catch { case _: Throwable => "unknown" }
        val more = s"Failed to read CSV at: $path (exists=true, size=$size bytes). Cause: ${e.getMessage}"
        throw new RuntimeException(more, e)
    }
  }
}
