package com.ledgerforge.pipeline.core

import com.ledgerforge.pipeline.utils.RuntimeCalculator
import com.ledgerforge.pipeline.config.CommandLineArgs
import com.ledgerforge.pipeline.reader.{CSVReader, ParquetReader}
import com.ledgerforge.pipeline.transformer.{AccountTransformer, BranchTransformer, CustomerTransformer, ProductTransformer, TransactionTransformer}
import com.ledgerforge.pipeline.writer.PostgresWriter
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}
import org.apache.spark.storage.StorageLevel

class JobRunner(cmdArgs: CommandLineArgs) {
  def run(): Unit = {
    val spark = SparkSessionProvider.getSparkSession("ETLJob")
    // Load the application configuration (application.conf). Previously this used
    // ConfigFactory.load(cmdArgs.env) which does not load the top-level `db` block
    // as expected; use load() to pick up application.conf and any environment-specific
    // overrides managed by Typesafe Config.
    val config = ConfigFactory.load()
    val reader = new CSVReader(spark)
    val parquetReader = new ParquetReader(spark)
    val writer = new PostgresWriter(config)

    val jobs = Seq(
      ("input.customer", new CustomerTransformer(), "customer_table"),
      ("input.account", new AccountTransformer(), "account_table"),
      ("input.transaction", new TransactionTransformer(), "transaction_table"),
      ("input.product", new ProductTransformer(), "product_table"),
      ("input.branch", new BranchTransformer(), "branch_table")
    )

    jobs.foreach { case (configKey, transformer, table) =>
      val rawPath = cmdArgs.inputOverride.getOrElse(config.getString(configKey))
      val inputPath = resolveInputPath(rawPath)
      val df =
        if (inputPath.toLowerCase.endsWith(".csv")) reader.read(inputPath)
        else RuntimeCalculator.calcRuntime(s"Reading Parquet from $inputPath") {
          parquetReader.read(inputPath)
        }
      // Persist the DataFrame before doing multiple actions so we avoid recomputation.
      // Use MEMORY_AND_DISK to be safer for larger datasets.
      val cached =  RuntimeCalculator.calcRuntime(s"persisting $inputPath") {
        df.persist(StorageLevel.MEMORY_AND_DISK)
      }

      // Materialize cache and get a count for logging
      val cnt = RuntimeCalculator.calcRuntime(s"counting $inputPath") {
        cached.count()
      }
      println(
        s"inputPath : $inputPath\n" +
          s"table     : $table\n" +
          s"count     : $cnt\n"
      )

      // Small data preview using the cached DataFrame
      RuntimeCalculator.calcRuntime(s"previewing $table") {
        cached.show(5)
      }

      // Use the cached DataFrame for transformations and writing
      val transformed = RuntimeCalculator.calcRuntime(s"Transforming $table") {
        transformer.transform(cached)
      }
      RuntimeCalculator.calcRuntime(s"writing $table")  {
        writer.write(transformed, table)
      }

      // Unpersist the cached input after downstream writes complete
      cached.unpersist(blocking = true)

    }
  }

  private def resolveInputPath(rawPath: String): String = {
    val asGiven = Paths.get(rawPath)
    // If the path is absolute and exists, use it as-is
    if (asGiven.isAbsolute && Files.exists(asGiven)) {
      return rawPath
    }
    // If a relative path exists in the current working directory, use it
    if (Files.exists(asGiven)) {
      return asGiven.toString
    }

    // Normalize the incoming path (strip leading /)
    val stripped = rawPath.stripPrefix("/")

    // Extract filename without directory or extension: 'data/customer.csv' -> 'customer'
    val fileName = Paths.get(stripped).getFileName.toString
    val baseName = {
      val idx = fileName.lastIndexOf('.')
      if (idx > 0) fileName.substring(0, idx) else fileName
    }

    // Prefer parquet directory under top-level data/synthetic/<baseName> if it exists
    val syntheticParquet = Paths.get("data", "synthetic", baseName)
    if (Files.exists(syntheticParquet)) {
      return syntheticParquet.toString
    }
    // If no exact match, try to find a directory that starts with the baseName (e.g., customer_scd2)
    val syntheticDir = Paths.get("data", "synthetic")
    if (Files.exists(syntheticDir) && Files.isDirectory(syntheticDir)) {
      val alt = java.nio.file.Files.newDirectoryStream(syntheticDir).iterator()
      while (alt.hasNext) {
        val candidate = alt.next().getFileName.toString
        if (candidate.startsWith(baseName)) return Paths.get("data", "synthetic", candidate).toString
      }
    }

    // Also check under resources (src/main/resources/data/<baseName>) which may contain parquet or directories
    val resourceCandidate = Paths.get("src/main/resources", "data", baseName)
    if (Files.exists(resourceCandidate)) {
      return resourceCandidate.toString
    }
    // Check for prefix matches in resources/data (e.g., customer_scd2)
    val resourcesDir = Paths.get("src/main/resources", "data")
    if (Files.exists(resourcesDir) && Files.isDirectory(resourcesDir)) {
      val iter = java.nio.file.Files.newDirectoryStream(resourcesDir).iterator()
      while (iter.hasNext) {
        val candidate = iter.next().getFileName.toString
        if (candidate.startsWith(baseName)) return resourcesDir.resolve(candidate).toString
      }
    }

    // Fallback: treat the original path as relative to src/main/resources (this matches previous behavior)
    Paths.get("src/main/resources").resolve(stripped).toString
  }
}
