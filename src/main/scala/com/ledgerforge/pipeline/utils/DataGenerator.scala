package com.ledgerforge.pipeline.utils

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate

object DataGenerator {

  final case class Config(
      outputPath: String = "data/synthetic",
      numTransactions: Long = 50000000L,
      numDays: Int = 30,
      numCustomers: Long = 5000000L,
      numAccounts: Long = 5000000L,
      numProducts: Int = 5000,
      numBranches: Int = 500,
      zipfExponent: Double = 1.2,
      duplicateRate: Double = 0.02,
      nullRate: Double = 0.02,
      invalidRate: Double = 0.03,
      customerChangeRate: Double = 0.15
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    val spark = SparkSession.builder()
      .appName("BankingDataGenerator")
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .getOrCreate()

    val startDate = LocalDate.now().minusDays(config.numDays.toLong - 1)
    val startDateStr = startDate.toString
    val scdStartStr = startDate.minusDays(365).toString

    val branches = generateBranches(spark, config, startDateStr)
    val products = generateProducts(spark, config)
    val accounts = generateAccounts(spark, config, startDateStr)
    val customers = generateCustomersSCD2(spark, config, startDateStr, scdStartStr)
    val transactions = generateTransactions(spark, config, startDateStr)

    branches.write.mode("overwrite").parquet(s"${config.outputPath}/branch")
    products.write.mode("overwrite").parquet(s"${config.outputPath}/product")
    accounts.write.mode("overwrite").parquet(s"${config.outputPath}/account")
    customers.write.mode("overwrite").parquet(s"${config.outputPath}/customer_scd2")
    transactions.write.mode("overwrite").partitionBy("event_date").parquet(s"${config.outputPath}/transaction")

    spark.stop()
  }

  private def parseArgs(args: Array[String]): Config = {
    if (args.isEmpty) {
      Config()
    } else {
      val kv = args.flatMap { arg =>
        arg.split("=", 2) match {
          case Array(k, v) => Some(k.trim -> v.trim)
          case _ => None
        }
      }.toMap

      Config(
        outputPath = kv.getOrElse("outputPath", "data/synthetic"),
        numTransactions = kv.get("numTransactions").map(_.toLong).getOrElse(50000000L),
        numDays = kv.get("numDays").map(_.toInt).getOrElse(30),
        numCustomers = kv.get("numCustomers").map(_.toLong).getOrElse(5000000L),
        numAccounts = kv.get("numAccounts").map(_.toLong).getOrElse(5000000L),
        numProducts = kv.get("numProducts").map(_.toInt).getOrElse(5000),
        numBranches = kv.get("numBranches").map(_.toInt).getOrElse(500),
        zipfExponent = kv.get("zipfExponent").map(_.toDouble).getOrElse(1.2),
        duplicateRate = kv.get("duplicateRate").map(_.toDouble).getOrElse(0.02),
        nullRate = kv.get("nullRate").map(_.toDouble).getOrElse(0.02),
        invalidRate = kv.get("invalidRate").map(_.toDouble).getOrElse(0.03),
        customerChangeRate = kv.get("customerChangeRate").map(_.toDouble).getOrElse(0.15)
      )
    }
  }

  private def generateBranches(spark: SparkSession, config: Config, startDateStr: String): DataFrame = {
    import spark.implicits._
    val branchNames = Array("Main", "Downtown", "Suburban", "Airport", "International", "Online")
    val states = Array("NY", "CA", "TX", "FL", "IL", "NJ", "WA", "MA")

    spark.range(1, config.numBranches.toLong + 1)
      .withColumn("branch_id", col("id").cast(LongType))
      .withColumn("branch_name", concat(lit("Branch "), element_at(array(branchNames.map(lit): _*), (rand(11) * branchNames.length + 1).cast("int"))))
      .withColumn("state", element_at(array(states.map(lit): _*), (rand(12) * states.length + 1).cast("int")))
      .withColumn("open_date", date_sub(to_date(lit(startDateStr)), (rand(13) * 3650).cast("int")))
      .drop("id")
      .transform(df => withNullOrInvalid(df, "state", config, lit("ZZ")))
  }

  private def generateProducts(spark: SparkSession, config: Config): DataFrame = {
    import spark.implicits._
    val productTypes = Array("Checking", "Savings", "Credit Card", "Debit Card", "Mortgage", "Auto Loan", "Personal Loan")

    spark.range(1, config.numProducts.toLong + 1)
      .withColumn("product_id", col("id").cast(LongType))
      .withColumn("product_name", element_at(array(productTypes.map(lit): _*), (rand(21) * productTypes.length + 1).cast("int")))
      .withColumn("product_group", when(col("product_name").contains("Loan"), lit("Loan")).otherwise(lit("Deposit")))
      .drop("id")
      .transform(df => withNullOrInvalid(df, "product_name", config, lit("UNKNOWN")))
  }

  private def generateAccounts(spark: SparkSession, config: Config, startDateStr: String): DataFrame = {
    import spark.implicits._
    val accountTypes = Array("Checking", "Savings", "Loan", "Credit")

    spark.range(1, config.numAccounts.toLong + 1)
      .withColumn("account_id", col("id").cast(LongType))
      .withColumn("customer_id", (rand(31) * config.numCustomers + 1).cast(LongType))
      .withColumn("branch_id", (rand(32) * config.numBranches + 1).cast(LongType))
      .withColumn("account_type", element_at(array(accountTypes.map(lit): _*), (rand(33) * accountTypes.length + 1).cast("int")))
      .withColumn("status", element_at(array(lit("Active"), lit("Dormant"), lit("Closed")), (rand(34) * 3 + 1).cast("int")))
      .withColumn("open_date", date_sub(to_date(lit(startDateStr)), (rand(35) * 3650).cast("int")))
      .withColumn("balance", round(rand(36) * 20000 - 2000, 2))
      .drop("id")
      .transform(df => withNullOrInvalid(df, "account_type", config, lit("INVALID_TYPE")))
      .transform(df => withNullOrInvalid(df, "customer_id", config, lit(-1L)))
  }

  private def generateCustomersSCD2(spark: SparkSession, config: Config, startDateStr: String, scdStartStr: String): DataFrame = {
    import spark.implicits._
    val firstNames = Array("John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry")
    val lastNames = Array("Doe", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez")

    val base = spark.range(1, config.numCustomers.toLong + 1)
      .withColumn("customer_id", col("id").cast(LongType))
      .withColumn("first_name", element_at(array(firstNames.map(lit): _*), (rand(41) * firstNames.length + 1).cast("int")))
      .withColumn("last_name", element_at(array(lastNames.map(lit): _*), (rand(42) * lastNames.length + 1).cast("int")))
      .withColumn("email", lower(concat(col("first_name"), lit("."), col("last_name"), col("customer_id"), lit("@example.com"))))
      .withColumn("phone", concat(lit("555-"), lpad((rand(43) * 1000).cast("int").cast("string"), 3, "0"), lit("-"), lpad((rand(44) * 10000).cast("int").cast("string"), 4, "0")))
      .withColumn("risk_tier", element_at(array(lit("Low"), lit("Medium"), lit("High")), (rand(45) * 3 + 1).cast("int")))
      .withColumn("base_start", date_add(to_date(lit(scdStartStr)), (rand(46) * 365).cast("int")))
      .withColumn("change_flag", rand(47) < lit(config.customerChangeRate))
      .withColumn("change_date", date_add(to_date(lit(startDateStr)), (rand(48) * config.numDays).cast("int")))
      .drop("id")
      .transform(df => withNullOrInvalid(df, "email", config, lit("invalid_email")))
      .transform(df => withNullOrInvalid(df, "phone", config, lit("000-000-0000")))

    val unchanged = base
      .filter(not(col("change_flag")))
      .withColumn("effective_start", col("base_start"))
      .withColumn("effective_end", lit(null).cast(DateType))
      .withColumn("is_current", lit(true))
      .withColumn("version", lit(1))

    val oldVersion = base
      .filter(col("change_flag"))
      .withColumn("effective_start", col("base_start"))
      .withColumn("effective_end", date_sub(col("change_date"), 1))
      .withColumn("is_current", lit(false))
      .withColumn("version", lit(1))

    val newVersion = base
      .filter(col("change_flag"))
      .withColumn("effective_start", col("change_date"))
      .withColumn("effective_end", lit(null).cast(DateType))
      .withColumn("is_current", lit(true))
      .withColumn("version", lit(2))
      .withColumn("risk_tier", when(col("risk_tier") === "High", lit("Medium")).otherwise(lit("High")))

    unchanged.unionByName(oldVersion).unionByName(newVersion)
      .drop("base_start", "change_flag", "change_date")
  }

  private def generateTransactions(spark: SparkSession, config: Config, startDateStr: String): DataFrame = {
    import spark.implicits._
    val txnTypes = Array("Deposit", "Withdrawal", "Transfer", "Payment")
    val currencies = Array("USD", "EUR", "GBP", "JPY", "CAD")

    val base = spark.range(0, config.numTransactions)
      .withColumn("base_transaction_id", col("id") + 1)
      .withColumn("event_date", date_add(to_date(lit(startDateStr)), (col("id") % config.numDays).cast("int")))
      .withColumn("event_ts", expr("timestamp(event_date) + interval 1 second * cast(rand(61) * 86400 as int)"))
      .withColumn("ingest_date", date_add(col("event_date"), (rand(62) * 4).cast("int")))
      .withColumn("account_id", zipfAccountId(config.numAccounts, config.zipfExponent, rand(63)))
      .withColumn("product_id", (rand(64) * config.numProducts + 1).cast(LongType))
      .withColumn("branch_id", (rand(65) * config.numBranches + 1).cast(LongType))
      .withColumn("amount", round(rand(66) * 2000 - 500, 2))
      .withColumn("currency", element_at(array(currencies.map(lit): _*), (rand(67) * currencies.length + 1).cast("int")))
      .withColumn("transaction_type", element_at(array(txnTypes.map(lit): _*), (rand(68) * txnTypes.length + 1).cast("int")))
      .withColumn("status", element_at(array(lit("Approved"), lit("Declined"), lit("Reversed")), (rand(69) * 3 + 1).cast("int")))
      .drop("id")

    base
      .withColumn("transaction_id", duplicateId(col("base_transaction_id"), config.duplicateRate))
      .transform(df => withNullOrInvalid(df, "account_id", config, lit(-1L)))
      .transform(df => withNullOrInvalid(df, "currency", config, lit("XXX")))
      .transform(df => withNullOrInvalid(df, "amount", config, lit(-999999.99)))
      .transform(df => withNullOrInvalid(df, "transaction_type", config, lit("UNKNOWN")))
      .drop("base_transaction_id")
  }

  private def duplicateId(baseId: Column, duplicateRate: Double): Column = {
    val offset = (rand(71) * 1000).cast("long")
    when(rand(72) < lit(duplicateRate), greatest(lit(1L), baseId - offset)).otherwise(baseId)
  }

  private def zipfAccountId(maxId: Long, exponent: Double, u: Column): Column = {
    val zipf = floor(pow(u, lit(-1.0 / exponent))).cast("long")
    ((zipf - 1) % maxId + 1).cast("long")
  }

  private def withNullOrInvalid(df: DataFrame, colName: String, config: Config, invalidValue: Column): DataFrame = {
    val nullCut = config.nullRate
    val invalidCut = config.nullRate + config.invalidRate
    val r = rand()
    df.withColumn(
      colName,
      when(r < lit(nullCut), lit(null).cast(df.schema(colName).dataType))
        .when(r < lit(invalidCut), invalidValue.cast(df.schema(colName).dataType))
        .otherwise(col(colName))
    )
  }
}
