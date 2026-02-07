package com.ledgerforge.pipeline.core

import com.ledgerforge.pipeline.config.CommandLineArgs
import com.ledgerforge.pipeline.reader.CSVReader
import com.ledgerforge.pipeline.transformer.{AccountTransformer, BranchTransformer, CustomerTransformer, ProductTransformer, TransactionTransformer}
import com.ledgerforge.pipeline.writer.PostgresWriter
import com.ledgerforge.pipeline.transformer._
import com.typesafe.config.ConfigFactory

class JobRunner(cmdArgs: CommandLineArgs) {
  def run(): Unit = {
    val spark = SparkSessionProvider.getSparkSession("ETLJob")
    // Load the application configuration (application.conf). Previously this used
    // ConfigFactory.load(cmdArgs.env) which does not load the top-level `db` block
    // as expected; use load() to pick up application.conf and any environment-specific
    // overrides managed by Typesafe Config.
    val config = ConfigFactory.load()
    val reader = new CSVReader(spark)
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
      val inputPath = if (rawPath.startsWith("src/main/resources")) rawPath else s"src/main/resources/${rawPath.stripPrefix("/")}"
      val df = reader.read(inputPath)
      println("inputPath: " + inputPath+"\ntable"+ table+"\n configKey" + configKey)
      df.show()
      val transformed = transformer.transform(df)
      writer.write(transformed, table)
    }
  }
}