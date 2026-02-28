# LedgerForge — Solutions Guide

> Solutions for all 24 problem statements (batch + streaming). Each solution includes Scala/Spark code, plan analysis notes, and key takeaways.

---

## Section A — Joins & Lookups

### Solution 1: Branch-Level Transaction Summary

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

object Problem01_BranchSummary {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem01")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val accounts     = spark.read.parquet("data/synthetic/account")
    val branches     = spark.read.parquet("data/synthetic/branch")

    val result = RuntimeCalculator.calcRuntime("Branch Summary") {
      transactions
        .filter(col("status") =!= "Reversed")                     // exclude reversed txns
        .filter(col("account_id").isNotNull && col("account_id") > 0) // exclude invalid
        .join(accounts, Seq("account_id"), "inner")                // txn → account
        .filter(col("branch_id").isNotNull && col("branch_id") > 0)
        .join(branches, Seq("branch_id"), "inner")                 // account → branch
        .groupBy("branch_id", "branch_name", "state")
        .agg(
          sum("amount").as("total_amount"),
          count("*").as("txn_count"),
          round(avg("amount"), 2).as("avg_amount")
        )
        .orderBy(desc("total_amount"))
    }

    result.show(20)
    spark.stop()
  }
}
```

**Key takeaways:**
- Filter before join to reduce shuffle volume.
- `Seq("account_id")` equi-join drops the duplicate column automatically.
- Spark pushes the `status =!= "Reversed"` filter down before the join (predicate pushdown).

---

### Solution 2: Broadcast Join — Product Enrichment

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

object Problem02_BroadcastJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem02")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val products     = spark.read.parquet("data/synthetic/product")

    // --- Without broadcast hint ---
    println("=== Without broadcast ===")
    val withoutBroadcast = RuntimeCalculator.calcRuntime("No-broadcast join") {
      transactions.join(products, Seq("product_id"), "left")
    }
    withoutBroadcast.explain(true) // look for SortMergeJoin + 2x Exchange
    println(s"Count: ${withoutBroadcast.count()}")

    // --- With broadcast hint ---
    println("\n=== With broadcast ===")
    val withBroadcast = RuntimeCalculator.calcRuntime("Broadcast join") {
      transactions.join(broadcast(products), Seq("product_id"), "left")
    }
    withBroadcast.explain(true) // look for BroadcastHashJoin, no Exchange on left
    println(s"Count: ${withBroadcast.count()}")

    // Handle nulls: fill missing product info
    val enriched = transactions
      .join(broadcast(products), Seq("product_id"), "left")
      .withColumn("product_name", coalesce(col("product_name"), lit("Unknown")))
      .withColumn("product_group", coalesce(col("product_group"), lit("Unknown")))

    enriched.show(5)
    spark.stop()
  }
}
```

**Key takeaways:**
- `broadcast(products)` forces Spark to broadcast the small DataFrame to all executors.
- Without broadcast: `SortMergeJoin` with 2 `Exchange` (shuffle) stages.
- With broadcast: `BroadcastHashJoin` with 0 shuffles on the large side.
- Spark auto-broadcasts DataFrames < `spark.sql.autoBroadcastJoinThreshold` (default 10MB), but `product` with 5K rows will usually be auto-broadcast anyway. The hint guarantees it.

---

### Solution 3: SCD2 Temporal Join

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

object Problem03_SCD2Join {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem03")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val accounts     = spark.read.parquet("data/synthetic/account")
    val customers    = spark.read.parquet("data/synthetic/customer_scd2")

    // Step 1: Get customer_id from account
    val txnWithCustomer = transactions
      .join(broadcast(accounts.select("account_id", "customer_id")), Seq("account_id"), "inner")

    // Step 2: Normalize effective_end (NULL means current → far future)
    val customersNormalized = customers
      .withColumn("effective_end_safe",
        coalesce(col("effective_end"), lit("9999-12-31").cast("date")))

    // Step 3: Range join — this is an inequality join, no equi-join optimization
    val enriched = RuntimeCalculator.calcRuntime("SCD2 temporal join") {
      txnWithCustomer.join(
        customersNormalized,
        txnWithCustomer("customer_id") === customersNormalized("customer_id") &&
          txnWithCustomer("event_date") >= customersNormalized("effective_start") &&
          txnWithCustomer("event_date") <= customersNormalized("effective_end_safe"),
        "inner"
      ).select(
        txnWithCustomer("transaction_id"),
        txnWithCustomer("event_date"),
        txnWithCustomer("amount"),
        txnWithCustomer("customer_id"),
        customersNormalized("first_name"),
        customersNormalized("last_name"),
        customersNormalized("risk_tier"),
        customersNormalized("version")
      )
    }

    enriched.show(10)
    println(s"Enriched count: ${enriched.count()}")
    spark.stop()
  }
}
```

**Key takeaways:**
- SCD2 joins are **range joins** — Spark cannot use hash/sort-merge for the inequality part.
- The equi-join on `customer_id` helps: Spark first partitions by `customer_id`, then applies the range condition.
- For very large datasets, consider bucketing both sides by `customer_id` to avoid the shuffle.
- Alternative: use `between` as a filter after an equi-join: join on `customer_id` first, then filter by date range.

---

## Section B — Window Functions & Deduplication

### Solution 4: Transaction Deduplication

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem04_Dedup {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem04")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val totalBefore = transactions.count()

    // Window: partition by transaction_id, order by ingest_date DESC, event_ts DESC
    val w = Window
      .partitionBy("transaction_id")
      .orderBy(col("ingest_date").desc, col("event_ts").desc)

    val deduped = RuntimeCalculator.calcRuntime("Dedup with row_number") {
      transactions
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") === 1)
        .drop("rn")
    }

    val totalAfter = deduped.count()
    println(s"Before: $totalBefore | After: $totalAfter | Removed: ${totalBefore - totalAfter}")

    // Comparison: dropDuplicates keeps an ARBITRARY row (no ordering guarantee)
    val dedupedSimple = RuntimeCalculator.calcRuntime("Dedup with dropDuplicates") {
      transactions.dropDuplicates("transaction_id")
    }
    println(s"dropDuplicates count: ${dedupedSimple.count()}")

    spark.stop()
  }
}
```

**Key takeaways:**
- `row_number()` gives deterministic dedup — you control which row survives via `orderBy`.
- `dropDuplicates` keeps an arbitrary row — fine if all duplicates are identical, but **wrong** when rows differ (e.g., different `ingest_date`).
- The `row_number()` approach requires a shuffle (to partition by `transaction_id`) + sort. This is unavoidable for correct dedup.

---

### Solution 5: Running Account Balance

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem05_RunningBalance {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem05")

    val transactions = spark.read.parquet("data/synthetic/transaction")

    val w = Window
      .partitionBy("account_id")
      .orderBy("event_date", "event_ts")

    val result = RuntimeCalculator.calcRuntime("Running balance") {
      transactions
        .withColumn("running_balance", sum("amount").over(w))
        .withColumn("txn_rank", row_number().over(w))
        .select("account_id", "transaction_id", "event_date", "amount",
                "running_balance", "txn_rank")
    }

    // Show a sample account to verify ordering
    result.filter(col("account_id") === 1).orderBy("txn_rank").show(20)

    // Observe skew: which accounts have the most transactions?
    transactions.groupBy("account_id").count()
      .orderBy(desc("count"))
      .show(10)

    spark.stop()
  }
}
```

**Key takeaways:**
- `sum("amount").over(w)` computes a cumulative sum — Spark uses the default frame `UNBOUNDED PRECEDING` to `CURRENT ROW` when `orderBy` is specified.
- Zipf-skewed `account_id` means a few partitions (for hot accounts) will be enormous — check Spark UI for task duration variance.
- Tuning `spark.sql.shuffle.partitions`: lower = fewer tasks but larger partitions; higher = more tasks but more scheduling overhead.

---

### Solution 6: Fraud Velocity Detection

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem06_VelocityDetection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem06")

    val transactions = spark.read.parquet("data/synthetic/transaction")
      .filter(col("status") === "Approved") // pre-filter to reduce volume

    // Convert event_ts to epoch seconds for rangeBetween
    val txnWithEpoch = transactions
      .withColumn("event_ts_epoch", col("event_ts").cast("long"))

    // Window: ±120 seconds (2 minutes) range
    val w = Window
      .partitionBy("account_id")
      .orderBy("event_ts_epoch")
      .rangeBetween(-120, 120)

    val result = RuntimeCalculator.calcRuntime("Velocity detection") {
      txnWithEpoch
        .withColumn("velocity_count", count("*").over(w))
        .withColumn("is_velocity_alert", col("velocity_count") >= 5)
        .select("transaction_id", "account_id", "event_ts", "amount",
                "velocity_count", "is_velocity_alert")
    }

    // Show alerts
    val alerts = result.filter(col("is_velocity_alert"))
    println(s"Total alerts: ${alerts.count()}")
    alerts.show(20)

    spark.stop()
  }
}
```

**Key takeaways:**
- `rangeBetween(-120, 120)` creates a 4-minute window (±2 min) based on the `orderBy` column's numeric value.
- This requires the `orderBy` column to be numeric (epoch seconds), not a timestamp.
- Very expensive for hot accounts due to skew — each row must scan all rows within its window.
- Pre-filtering by `status = "Approved"` significantly reduces computation.

---

## Section C — Aggregations & Data Quality

### Solution 7: Daily Customer Spend with Late Data

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import java.io.{File, PrintWriter}

object Problem07_DailySpend {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem07")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val watermarkFile = "output/watermark.txt"
    val outputPath = "output/daily_spend"
    val lookbackDays = 3

    // Read watermark or default to 30 days ago
    val runDate = LocalDate.now()
    val lastWatermark = readWatermark(watermarkFile, runDate.minusDays(30))
    val startDate = lastWatermark.minusDays(lookbackDays) // reprocess window
    val endDate = runDate

    println(s"Processing window: $startDate to $endDate")

    // Read with partition pruning
    val transactions = spark.read.parquet("data/synthetic/transaction")
      .filter(col("event_date").between(startDate.toString, endDate.toString))

    // Verify partition pruning
    transactions.explain(true) // look for PartitionFilters in the plan

    // Dedup
    val w = Window.partitionBy("transaction_id").orderBy(col("ingest_date").desc)
    val deduped = transactions
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .drop("rn")

    // Join to get customer_id
    val accounts = spark.read.parquet("data/synthetic/account")
      .select("account_id", "customer_id")
    val withCustomer = deduped.join(broadcast(accounts), Seq("account_id"), "inner")

    // Aggregate
    val dailySpend = RuntimeCalculator.calcRuntime("Daily spend aggregation") {
      withCustomer
        .groupBy("customer_id", "event_date")
        .agg(
          sum("amount").as("total_spend"),
          count("*").as("txn_count")
        )
    }

    // Write — dynamic partition overwrite ensures idempotency
    dailySpend.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    // Update watermark
    writeWatermark(watermarkFile, endDate)
    println(s"Watermark updated to $endDate")

    spark.stop()
  }

  private def readWatermark(path: String, default: LocalDate): LocalDate = {
    val f = new File(path)
    if (f.exists()) {
      val dateStr = Source.fromFile(f).getLines().next().trim
      LocalDate.parse(dateStr)
    } else default
  }

  private def writeWatermark(path: String, date: LocalDate): Unit = {
    new File(path).getParentFile.mkdirs()
    val pw = new PrintWriter(path)
    pw.println(date.toString)
    pw.close()
  }
}
```

**Key takeaways:**
- `partitionOverwriteMode = dynamic` overwrites only the partitions being written, not the entire output directory.
- Partition pruning: when the Parquet is partitioned by `event_date`, the `filter(col("event_date").between(...))` is pushed to the file listing level — Spark skips reading irrelevant partition directories entirely.
- The 3-day lookback window handles late-arriving data by reprocessing recent history.
- Idempotency: running twice for the same date produces the same result because we overwrite the same partitions.

---

### Solution 8: Data Quality Quarantine Pipeline

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator

object Problem08_DataQuality {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem08")

    // Accumulators for quality metrics
    val nullTxnId     = spark.sparkContext.longAccumulator("null_transaction_id")
    val invalidAcctId = spark.sparkContext.longAccumulator("invalid_account_id")
    val invalidAmount = spark.sparkContext.longAccumulator("invalid_amount")
    val invalidCcy    = spark.sparkContext.longAccumulator("invalid_currency")
    val nullDate      = spark.sparkContext.longAccumulator("null_event_date")
    val invalidType   = spark.sparkContext.longAccumulator("invalid_txn_type")

    val transactions = spark.read.parquet("data/synthetic/transaction")

    // Define quality rule columns (true = passes the rule)
    val rules = Map(
      "valid_txn_id"   -> col("transaction_id").isNotNull,
      "valid_acct_id"  -> (col("account_id").isNotNull && col("account_id") > 0),
      "valid_amount"   -> (col("amount").isNotNull && col("amount") =!= -999999.99),
      "valid_currency" -> (col("currency").isNotNull && col("currency") =!= "XXX"),
      "valid_date"     -> col("event_date").isNotNull,
      "valid_type"     -> (col("transaction_type").isNotNull && col("transaction_type") =!= "UNKNOWN")
    )

    // Add rule columns
    var df = transactions
    rules.foreach { case (name, expr) => df = df.withColumn(name, expr) }

    // All rules must pass for a record to be valid
    val allValid = rules.keys.map(col).reduce(_ && _)
    df = df.withColumn("is_valid", allValid)

    // Build rejection_reasons array
    val reasonExprs = rules.map { case (name, _) =>
      when(!col(name), lit(name.replace("valid_", "failed_")))
    }.toSeq
    df = df.withColumn("rejection_reasons",
      array(reasonExprs: _*))
    df = df.withColumn("rejection_reasons",
      expr("filter(rejection_reasons, x -> x is not null)"))

    // Split
    val validDf = df.filter(col("is_valid")).drop(rules.keys.toSeq ++ Seq("is_valid", "rejection_reasons"): _*)
    val quarantineDf = df.filter(!col("is_valid")).drop(rules.keys.toSeq ++ Seq("is_valid"): _*)

    RuntimeCalculator.calcRuntime("Quality check") {
      val validCount = validDf.count()
      val quarantineCount = quarantineDf.count()
      val total = validCount + quarantineCount

      println(s"\n=== Data Quality Summary ===")
      println(s"Total records:       $total")
      println(s"Valid records:       $validCount (${validCount * 100.0 / total}%)")
      println(s"Quarantined records: $quarantineCount (${quarantineCount * 100.0 / total}%)")
    }

    quarantineDf.show(10, truncate = false)

    // Write quarantine
    quarantineDf.write.mode("overwrite").parquet("output/quarantine")

    spark.stop()
  }
}
```

**Key takeaways:**
- All quality rules are expressed as **column expressions** — no row-by-row UDFs needed.
- `array(...)` + `filter(... is not null)` builds a dynamic list of rejection reasons per row.
- Accumulators give pipeline-level counts but run lazily — they're only updated when an action triggers.
- This pattern separates concerns: valid data flows downstream, quarantined data goes for review.

---

### Solution 9: UDF vs. Native — Risk Bucket

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object Problem09_UDFvsNative {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem09")

    val transactions = spark.read.parquet("data/synthetic/transaction")

    // --- Approach 1: UDF ---
    val classifyUDF: UserDefinedFunction = udf((txnType: String, amount: java.lang.Double) => {
      if (amount == null || txnType == null) "UNKNOWN"
      else if (amount > 5000) "CRITICAL"
      else if (amount > 1000 && txnType == "Transfer") "HIGH"
      else if (amount > 500) "MEDIUM"
      else "LOW"
    })

    val withUDF = RuntimeCalculator.calcRuntime("UDF approach") {
      val df = transactions.withColumn("risk_bucket", classifyUDF(col("transaction_type"), col("amount")))
      df.groupBy("risk_bucket").count().collect() // force execution
      df
    }
    withUDF.explain(true) // look for absence of WholeStageCodegen around the UDF

    // --- Approach 2: Native expressions ---
    val withNative = RuntimeCalculator.calcRuntime("Native approach") {
      val df = transactions.withColumn("risk_bucket",
        when(col("amount").isNull || col("transaction_type").isNull, lit("UNKNOWN"))
          .when(col("amount") > 5000, lit("CRITICAL"))
          .when(col("amount") > 1000 && col("transaction_type") === "Transfer", lit("HIGH"))
          .when(col("amount") > 500, lit("MEDIUM"))
          .otherwise(lit("LOW"))
      )
      df.groupBy("risk_bucket").count().collect() // force execution
      df
    }
    withNative.explain(true) // look for WholeStageCodegen wrapping the entire pipeline

    spark.stop()
  }
}
```

**Key takeaways:**
- **UDFs break Catalyst optimizations**: the optimizer treats UDFs as black boxes — no predicate pushdown, no constant folding, no codegen.
- **Native expressions enable WholeStageCodegen**: Spark generates Java bytecode for the entire stage, running 2-10x faster.
- Always prefer `when/otherwise` chains over UDFs for conditional logic.
- UDFs also require serialization/deserialization of data between JVM and Spark's internal format (Tungsten).

---

## Section D — Partitioning, Bucketing & Skew

### Solution 10: Partition Optimization

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

object Problem10_PartitionOptimization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem10")

    val transactions = spark.read.parquet("data/synthetic/transaction")

    // Count files per partition
    val files = transactions.inputFiles
    println(s"Total files: ${files.length}")

    // File distribution per event_date
    val fileDist = transactions
      .select("event_date")
      .distinct()
      .orderBy("event_date")
    println(s"Number of date partitions: ${fileDist.count()}")

    // Rewrite with controlled file count: 4 files per date partition
    val outputPath = "output/transaction_optimized"

    RuntimeCalculator.calcRuntime("Rewrite with repartition") {
      transactions
        .repartition(4, col("event_date")) // 4 partitions per unique date
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(outputPath)
    }

    // Verify
    val optimized = spark.read.parquet(outputPath)
    println(s"Optimized files: ${optimized.inputFiles.length}")

    // Compare read performance
    RuntimeCalculator.calcRuntime("Original full count") {
      transactions.count()
    }
    RuntimeCalculator.calcRuntime("Optimized full count") {
      optimized.count()
    }

    spark.stop()
  }
}
```

**Key takeaways:**
- `repartition(4, col("event_date"))` creates 4 partitions **total**, not 4 per date. To get 4 files per date, the total partitions should be `4 * numDates`.
- Better approach: write with `maxRecordsPerFile`: `.option("maxRecordsPerFile", 1000000)`.
- `coalesce(N)` reduces partitions without a full shuffle (narrow dependency), but combined with `repartition` it may not give the expected result since `repartition` already set the count.

---

### Solution 11: Salting for Skewed Joins

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

object Problem11_SaltedJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem11")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val accounts     = spark.read.parquet("data/synthetic/account")
    val saltBuckets  = 10

    // --- Regular join (skewed) ---
    val regularResult = RuntimeCalculator.calcRuntime("Regular join (skewed)") {
      val df = transactions.join(accounts, Seq("account_id"), "inner")
      df.count()
      df
    }

    // --- Salted join ---
    // Step 1: Add salt to transactions
    val saltedTxn = transactions
      .withColumn("salt", (rand() * saltBuckets).cast("int"))

    // Step 2: Explode accounts to have one copy per salt value
    val saltedAccounts = accounts
      .withColumn("salt", explode(array((0 until saltBuckets).map(lit(_)): _*)))

    // Step 3: Join on (account_id, salt)
    val saltedResult = RuntimeCalculator.calcRuntime("Salted join") {
      val df = saltedTxn
        .join(saltedAccounts, Seq("account_id", "salt"), "inner")
        .drop("salt")
      df.count()
      df
    }

    println(s"Regular result count: ${regularResult.count()}")
    println(s"Salted result count:  ${saltedResult.count()}")

    spark.stop()
  }
}
```

**Key takeaways:**
- Salting distributes hot keys across `N` buckets, turning 1 skewed task into N balanced tasks.
- The trade-off: the dimension table (`accounts`) is replicated N times, increasing memory/shuffle on that side.
- Choose `N` based on the skew ratio: if the hottest key has 100x more rows than median, try N=10-20.
- Spark 3.x AQE with `skewJoin.enabled = true` can handle this automatically in some cases.

---

### Solution 12: Bucketed Join

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator

object Problem12_BucketedJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem12")
    spark.conf.set("spark.sql.sources.bucketing.enabled", "true")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val accounts     = spark.read.parquet("data/synthetic/account")

    // Write bucketed tables
    RuntimeCalculator.calcRuntime("Write bucketed accounts") {
      accounts.write
        .mode("overwrite")
        .bucketBy(16, "account_id")
        .sortBy("account_id")
        .saveAsTable("account_bucketed")
    }

    RuntimeCalculator.calcRuntime("Write bucketed transactions") {
      transactions.write
        .mode("overwrite")
        .bucketBy(16, "account_id")
        .sortBy("account_id")
        .saveAsTable("transaction_bucketed")
    }

    // Join bucketed tables — should have NO Exchange (shuffle)
    val bucketedAccounts = spark.table("account_bucketed")
    val bucketedTxns     = spark.table("transaction_bucketed")

    val result = RuntimeCalculator.calcRuntime("Bucketed join") {
      val df = bucketedTxns.join(bucketedAccounts, Seq("account_id"), "inner")
      df.count()
      df
    }

    // Verify no shuffle in the plan
    result.explain(true)
    // Look for SortMergeJoin WITHOUT Exchange nodes before it

    spark.stop()
  }
}
```

**Key takeaways:**
- Both tables must use the **same number of buckets** and the **same bucket column** for shuffle-free joins.
- `saveAsTable` writes to the Spark warehouse directory (not arbitrary paths).
- Bucketing pays off when the same join is executed repeatedly (daily pipeline).
- The physical plan should show `SortMergeJoin` directly reading from bucketed files — no `Exchange`.

---

## Section E — Performance Tuning & Caching

### Solution 13: AQE Experiment

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

object Problem13_AQE {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem13")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val accounts     = spark.read.parquet("data/synthetic/account")

    def runPipeline(label: String): Unit = {
      val result = RuntimeCalculator.calcRuntime(label) {
        val df = transactions
          .join(accounts, Seq("account_id"), "inner")
          .groupBy("branch_id")
          .agg(sum("amount").as("total_amount"), count("*").as("txn_count"))
        df.count()
        df
      }
      result.explain(true)
      println(s"$label partitions: ${result.rdd.getNumPartitions}")
    }

    // Run 1: AQE OFF
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    runPipeline("AQE=OFF, partitions=200")

    // Run 2: AQE ON
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    runPipeline("AQE=ON, partitions=200")

    // Run 3: AQE ON + Skew Join
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    runPipeline("AQE=ON + SkewJoin")

    spark.stop()
  }
}
```

**Key takeaways:**
- **AQE OFF**: Spark uses the static 200 shuffle partitions, even if most are empty after the aggregation.
- **AQE ON**: Spark dynamically coalesces the 200 partitions down to a smaller number based on actual data sizes. Look for `CustomShuffleReader` or `AQEShuffleRead` in the plan.
- **AQE + Skew Join**: Spark detects skewed partitions and splits them into sub-partitions. Look for `SkewJoin` in the plan.
- `spark.sql.adaptive.coalescePartitions.minPartitionSize` controls the minimum partition size after coalescing (default 1MB).

---

### Solution 14: Caching Strategy Comparison

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Problem14_CachingStrategies {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem14")
    spark.sparkContext.setCheckpointDir("output/checkpoints")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val w = Window.partitionBy("transaction_id").orderBy(col("ingest_date").desc)
    val deduped = transactions.withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1).drop("rn")

    def runThreeActions(df: org.apache.spark.sql.DataFrame, label: String): Unit = {
      RuntimeCalculator.calcRuntime(s"$label - 3 actions") {
        println(s"  Count: ${df.count()}")
        println(s"  Approved: ${df.filter(col("status") === "Approved").count()}")
        println(s"  Avg amount: ${df.agg(avg("amount")).first().getDouble(0)}")
      }
    }

    // Strategy 1: No caching
    runThreeActions(deduped, "No cache")

    // Strategy 2: .cache() = MEMORY_AND_DISK
    val cached = deduped.cache()
    runThreeActions(cached, "cache()")
    cached.unpersist(blocking = true)

    // Strategy 3: DISK_ONLY
    val diskOnly = deduped.persist(StorageLevel.DISK_ONLY)
    runThreeActions(diskOnly, "DISK_ONLY")
    diskOnly.unpersist(blocking = true)

    // Strategy 4: Checkpoint (breaks lineage)
    val checkpointed = deduped
    checkpointed.count() // materialize before checkpoint
    val cp = checkpointed.checkpoint(eager = true)
    runThreeActions(cp, "checkpoint")

    spark.stop()
  }
}
```

**Key takeaways:**

| Strategy | Speed | Memory Use | Lineage | Best For |
|----------|-------|------------|---------|----------|
| No cache | Slowest (recomputes) | None | Full | One-time use |
| `.cache()` | Fast (memory+disk) | High | Preserved | Multi-use, fits in memory |
| `DISK_ONLY` | Medium | None | Preserved | Large data, memory-constrained |
| `.checkpoint()` | Medium | None | **Broken** | Very long lineage chains |

- `.cache()` is best when the DataFrame is used multiple times and fits in memory.
- `.checkpoint()` truncates the lineage graph — useful when the DAG is very deep and recomputation from scratch would be expensive if a task fails.

---

### Solution 15: Physical Plan Analysis

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.functions._

object Problem15_PlanAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem15")

    val transactions = spark.read.parquet("data/synthetic/transaction")
    val accounts     = spark.read.parquet("data/synthetic/account")
    val branches     = spark.read.parquet("data/synthetic/branch")

    // --- Naive pipeline ---
    println("=== NAIVE PLAN ===")
    val naive = transactions
      .join(accounts, Seq("account_id"))
      .join(branches, Seq("branch_id"))
      .groupBy("state")
      .agg(sum("amount").as("total"))
    naive.explain(true)
    // Count Exchange nodes in the plan
    val naivePlan = naive.queryExecution.executedPlan.toString()
    val naiveShuffles = "Exchange".r.findAllIn(naivePlan).length
    println(s"Naive shuffles: $naiveShuffles")

    RuntimeCalculator.calcRuntime("Naive pipeline") { naive.count() }

    // --- Optimized pipeline ---
    println("\n=== OPTIMIZED PLAN ===")
    val optimized = transactions
      .join(accounts, Seq("account_id"))           // SortMergeJoin (both large)
      .join(broadcast(branches), Seq("branch_id")) // Broadcast (500 rows!)
      .groupBy("state")
      .agg(sum("amount").as("total"))
    optimized.explain(true)
    val optPlan = optimized.queryExecution.executedPlan.toString()
    val optShuffles = "Exchange".r.findAllIn(optPlan).length
    println(s"Optimized shuffles: $optShuffles")

    RuntimeCalculator.calcRuntime("Optimized pipeline") { optimized.count() }

    spark.stop()
  }
}
```

**Key takeaways:**
- The naive plan has 4 `Exchange` nodes: 2 for the first join (both sides shuffled by `account_id`), 2 more for the second join (reshuffled by `branch_id`), plus 1 for the final aggregation.
- Broadcasting `branches` eliminates 2 shuffles (branch side doesn't need to be shuffled, and the left side retains its existing partitioning).
- Always broadcast dimension tables (< 10MB). Check `spark.sql.autoBroadcastJoinThreshold`.
- Use `queryExecution.executedPlan.toString()` to programmatically analyze plans.

---

## Section F — Incremental Processing & Production Patterns

### Solution 16: Incremental Partition Load

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

object Problem16_IncrementalLoad {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem16")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val watermarkFile = "output/incremental_watermark.txt"
    val outputPath = "output/incremental_transactions"

    val today = LocalDate.now()
    val lastWatermark = readWatermark(watermarkFile, today.minusDays(30))

    println(s"Last watermark: $lastWatermark, processing up to: $today")

    // Read only new partitions
    val newData = spark.read.parquet("data/synthetic/transaction")
      .filter(col("event_date") > lastWatermark.toString && col("event_date") <= today.toString)

    val newCount = newData.count()
    println(s"New records to process: $newCount")

    if (newCount > 0) {
      // Dedup
      val w = Window.partitionBy("transaction_id").orderBy(col("ingest_date").desc)
      val deduped = newData.withColumn("rn", row_number().over(w))
        .filter(col("rn") === 1).drop("rn")

      // Write with dynamic partition overwrite (idempotent)
      RuntimeCalculator.calcRuntime("Incremental write") {
        deduped.write
          .mode("overwrite")
          .partitionBy("event_date")
          .parquet(outputPath)
      }

      writeWatermark(watermarkFile, today)
      println(s"Watermark updated to $today")
    } else {
      println("No new data to process.")
    }

    spark.stop()
  }

  private def readWatermark(path: String, default: LocalDate): LocalDate = {
    val f = new File(path)
    if (f.exists()) LocalDate.parse(Source.fromFile(f).getLines().next().trim)
    else default
  }

  private def writeWatermark(path: String, date: LocalDate): Unit = {
    new File(path).getParentFile.mkdirs()
    val pw = new PrintWriter(path)
    pw.println(date.toString)
    pw.close()
  }
}
```

**Key takeaways:**
- Incremental loading reads only new partitions — O(new data) instead of O(all data).
- `partitionOverwriteMode = dynamic` ensures only touched partitions are overwritten.
- The watermark pattern is simple but effective for daily batch pipelines.
- Cold start: when no watermark exists, default to a reasonable lookback (30 days).

---

### Solution 17: Config-Driven Transformer

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.transformer.Transformer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import scala.collection.JavaConverters._

class ConfigDrivenTransformer(entityName: String, config: Config) extends Transformer {

  private val entityConfig: Option[Config] = {
    val path = s"transformations.$entityName"
    if (config.hasPath(path)) Some(config.getConfig(path)) else None
  }

  override def transform(df: DataFrame): DataFrame = {
    entityConfig match {
      case None => df // no config for this entity, pass through
      case Some(cfg) =>
        var result = df

        // Add columns
        if (cfg.hasPath("add_columns")) {
          val addCols = cfg.getConfigList("add_columns").asScala
          addCols.foreach { colCfg =>
            val name = colCfg.getString("name")
            val expression = colCfg.getString("expr")
            result = result.withColumn(name, expr(expression))
          }
        }

        // Rename columns
        if (cfg.hasPath("rename_columns")) {
          val renames = cfg.getConfig("rename_columns")
          renames.entrySet().asScala.foreach { entry =>
            result = result.withColumnRenamed(entry.getKey, entry.getValue.unwrapped().toString)
          }
        }

        // Drop columns
        if (cfg.hasPath("drop_columns")) {
          val drops = cfg.getStringList("drop_columns").asScala
          result = result.drop(drops: _*)
        }

        result
    }
  }
}

// Example usage:
object Problem17_ConfigTransformer {
  def main(args: Array[String]): Unit = {
    import com.ledgerforge.pipeline.core.SparkSessionProvider

    val spark = SparkSessionProvider.getSparkSession("Problem17")

    // Load config with transformations block
    val config = ConfigFactory.parseString(
      """
        |transformations {
        |  customer {
        |    add_columns = [
        |      { name = "full_name", expr = "concat(first_name, ' ', last_name)" },
        |      { name = "ingest_date", expr = "current_date()" }
        |    ]
        |    rename_columns = { risk_tier: customer_risk }
        |    drop_columns = ["version"]
        |  }
        |}
      """.stripMargin)

    val customers = spark.read.parquet("data/synthetic/customer_scd2")
    val transformer = new ConfigDrivenTransformer("customer", config)
    val result = transformer.transform(customers)

    result.printSchema()
    result.show(5)

    spark.stop()
  }
}
```

**Key takeaways:**
- `expr(string)` parses any valid Spark SQL expression — very powerful for config-driven pipelines.
- The transformer is reusable across entities — just add a new block in the config.
- Handles missing config sections gracefully (no crash).
- In production, store these configs in a version-controlled file, not inline.

---

### Solution 18: End-to-End Pipeline Orchestrator

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import com.ledgerforge.pipeline.utils.RuntimeCalculator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem18_PipelineOrchestrator {

  case class StageResult(name: String, inputCount: Long, outputCount: Long, durationMs: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem18")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    var stages = Seq.empty[StageResult]

    def runStage[T](name: String, inputCount: Long)(fn: => (DataFrame, Long)): DataFrame = {
      val start = System.nanoTime()
      val (df, outCount) = fn
      val durationMs = (System.nanoTime() - start) / 1_000_000
      stages = stages :+ StageResult(name, inputCount, outCount, durationMs)
      df
    }

    // Stage 1: Read
    val raw = spark.read.parquet("data/synthetic/transaction")
    val rawCount = raw.count()

    // Stage 2: Dedup
    val deduped = runStage("Dedup", rawCount) {
      val w = Window.partitionBy("transaction_id").orderBy(col("ingest_date").desc, col("event_ts").desc)
      val df = raw.withColumn("rn", row_number().over(w)).filter(col("rn") === 1).drop("rn")
      val c = df.count()
      (df, c)
    }
    deduped.cache() // used by quality check and enrich

    // Stage 3: Quality Check
    val (valid, quarantine) = {
      val isValid =
        col("transaction_id").isNotNull &&
        col("account_id").isNotNull && col("account_id") > 0 &&
        col("amount").isNotNull && col("amount") =!= -999999.99 &&
        col("currency").isNotNull && col("currency") =!= "XXX"

      val v = deduped.filter(isValid)
      val q = deduped.filter(!isValid)
      (v, q)
    }
    val validCount = valid.count()
    val quarantineCount = quarantine.count()
    stages = stages :+ StageResult("Quality Check", deduped.count(), validCount, 0)

    quarantine.write.mode("overwrite").parquet("output/pipeline/quarantine")

    // Stage 4: Enrich with product + branch (broadcast)
    val products = broadcast(spark.read.parquet("data/synthetic/product"))
    val branches = broadcast(spark.read.parquet("data/synthetic/branch"))
    val accounts = spark.read.parquet("data/synthetic/account")

    val enriched = runStage("Enrich", validCount) {
      val df = valid
        .join(accounts, Seq("account_id"), "left")
        .join(products, Seq("product_id"), "left")
        .join(branches, Seq("branch_id"), "left")
      val c = df.count()
      (df, c)
    }

    // Stage 5: SCD2 Join
    val customers = spark.read.parquet("data/synthetic/customer_scd2")
      .withColumn("effective_end_safe", coalesce(col("effective_end"), lit("9999-12-31").cast("date")))

    val withCustomer = runStage("SCD2 Join", enriched.count()) {
      val df = enriched.join(customers,
        enriched("customer_id") === customers("customer_id") &&
          enriched("event_date") >= customers("effective_start") &&
          enriched("event_date") <= customers("effective_end_safe"),
        "left"
      ).drop(customers("customer_id"))
      val c = df.count()
      (df, c)
    }

    // Stage 6: Aggregate — daily spend per customer
    val dailySpend = runStage("Aggregate", withCustomer.count()) {
      val df = withCustomer.groupBy(enriched("customer_id"), col("event_date"))
        .agg(sum("amount").as("total_spend"), count("*").as("txn_count"))
      val c = df.count()
      (df, c)
    }

    // Stage 7: Write
    RuntimeCalculator.calcRuntime("Write outputs") {
      withCustomer.write.mode("overwrite").partitionBy("event_date").parquet("output/pipeline/enriched")
      dailySpend.write.mode("overwrite").partitionBy("event_date").parquet("output/pipeline/daily_spend")
    }

    deduped.unpersist(blocking = true)

    // Print pipeline summary
    println("\n" + "=" * 80)
    println("PIPELINE SUMMARY")
    println("=" * 80)
    println(f"${"Stage"}%-20s ${"Input"}%12s ${"Output"}%12s ${"Duration"}%12s")
    println("-" * 60)
    stages.foreach { s =>
      println(f"${s.name}%-20s ${s.inputCount}%,12d ${s.outputCount}%,12d ${s.durationMs}%,10d ms")
    }
    val totalMs = stages.map(_.durationMs).sum
    println("-" * 60)
    println(f"${"TOTAL"}%-20s ${""}%12s ${""}%12s ${totalMs}%,10d ms")
    println("=" * 80)

    spark.stop()
  }
}
```

**Key takeaways:**
- The orchestrator chains stages and collects metrics for each one.
- Caching the deduped DataFrame avoids recomputing it for quality check and enrichment.
- Broadcast joins on small dimension tables (product, branch) eliminate shuffles.
- AQE handles partition coalescing and potential skew automatically.
- The pipeline summary table quickly identifies the bottleneck stage.
- In production, this pattern would be wrapped in an Airflow DAG for scheduling.

---

## Quick Reference: Spark Optimization Cheat Sheet

| Technique | When to Use | Spark Config / API |
|-----------|-------------|-------------------|
| Broadcast join | Dimension < 10MB | `broadcast(df)` |
| Salting | Skewed join keys | Manual: add salt + explode |
| Bucketing | Repeated joins on same key | `.bucketBy().saveAsTable()` |
| AQE | Always in Spark 3.x | `spark.sql.adaptive.enabled=true` |
| Partition pruning | Partitioned Parquet + filter | `filter(col("event_date") > ...)` |
| Cache | Multi-use DataFrame | `.cache()` / `.persist(level)` |
| Checkpoint | Very long lineage | `.checkpoint()` |
| Repartition | Control parallelism/file count | `.repartition(n, col)` |
| Coalesce | Reduce partitions (no shuffle) | `.coalesce(n)` |
| Native expressions | Always (avoid UDFs) | `when/otherwise`, `expr()` |
| Dynamic partition overwrite | Incremental writes | `partitionOverwriteMode=dynamic` |

---

## Section G — Spark Structured Streaming Solutions

### Solution 19: Streaming Ingestion with Watermarking

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Problem19_StreamingWatermark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem19")

    // Define schema explicitly (required for readStream)
    val txnSchema = new StructType()
      .add("transaction_id", LongType)
      .add("account_id", LongType)
      .add("product_id", LongType)
      .add("branch_id", LongType)
      .add("amount", DoubleType)
      .add("currency", StringType)
      .add("transaction_type", StringType)
      .add("status", StringType)
      .add("event_date", DateType)
      .add("event_ts", TimestampType)
      .add("ingest_date", DateType)

    // Read as a stream — maxFilesPerTrigger simulates streaming pace
    val stream = spark.readStream
      .schema(txnSchema)
      .option("maxFilesPerTrigger", 2)
      .parquet("data/synthetic/transaction")

    // Watermark: ignore data arriving more than 3 days late
    // Tumbling window: 1-hour buckets
    val hourlyAgg = stream
      .withWatermark("event_ts", "3 days")
      .groupBy(
        window(col("event_ts"), "1 hour"),
        col("account_id")
      )
      .agg(
        sum("amount").as("hourly_total"),
        count("*").as("txn_count")
      )

    // Write to Parquet with checkpointing
    val query = hourlyAgg.writeStream
      .outputMode("append") // append — finalized windows only
      .format("parquet")
      .option("path", "output/streaming/hourly_agg")
      .option("checkpointLocation", "output/streaming/checkpoint_19")
      .start()

    // Run for 60 seconds then stop (for testing)
    query.awaitTermination(60000)
    query.stop()
    spark.stop()
  }
}
```

**Key takeaways:**
- `readStream` requires an explicit schema — it cannot infer from streaming sources.
- `maxFilesPerTrigger = 2` limits how many files are processed per micro-batch, simulating a real streaming pace.
- `withWatermark("event_ts", "3 days")` tells Spark: "any event with `event_ts` older than 3 days behind the latest seen event can be dropped."
- `append` output mode means Spark only outputs a window's result **after the watermark passes it** (the window is "closed" and finalized).
- The checkpoint directory stores offsets and state so the stream can recover from failures exactly where it left off.

---

### Solution 20: Real-Time Fraud Velocity Alert (Streaming)

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Problem20_StreamingVelocity {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem20")

    val txnSchema = new StructType()
      .add("transaction_id", LongType)
      .add("account_id", LongType)
      .add("amount", DoubleType)
      .add("status", StringType)
      .add("event_ts", TimestampType)

    val stream = spark.readStream
      .schema(txnSchema)
      .option("maxFilesPerTrigger", 2)
      .parquet("data/synthetic/transaction")
      .filter(col("status") === "Approved")

    // 2-minute tumbling window per account
    val velocity = stream
      .withWatermark("event_ts", "10 minutes")
      .groupBy(
        window(col("event_ts"), "2 minutes"),
        col("account_id")
      )
      .count()
      .filter(col("count") >= 5)
      .withColumnRenamed("count", "velocity_count")

    // Output to console for debugging
    val query = velocity.writeStream
      .outputMode("update") // update — emit changed rows each micro-batch
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "output/streaming/checkpoint_20")
      .start()

    query.awaitTermination(60000)
    query.stop()
    spark.stop()
  }
}
```

**Key takeaways:**
- **Batch (Problem 6)** uses `rangeBetween(-120, 120)` — a sliding window that counts neighbors per row. Complex and expensive.
- **Streaming version** uses `window("event_ts", "2 minutes")` — a tumbling window that groups rows into fixed 2-minute buckets. Simpler and more natural.
- `update` mode emits every row that changed in the current micro-batch (vs. `append` which waits for watermark to close the window).
- The 10-minute watermark means Spark drops state for windows older than 10 minutes, keeping memory bounded.

---

### Solution 21: Stream-Static Join — Enrich Transactions

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Problem21_StreamStaticJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem21")

    // Static dimension tables — loaded ONCE
    val products = spark.read.parquet("data/synthetic/product")
    val branches = spark.read.parquet("data/synthetic/branch")

    val txnSchema = new StructType()
      .add("transaction_id", LongType)
      .add("account_id", LongType)
      .add("product_id", LongType)
      .add("branch_id", LongType)
      .add("amount", DoubleType)
      .add("currency", StringType)
      .add("transaction_type", StringType)
      .add("status", StringType)
      .add("event_date", DateType)
      .add("event_ts", TimestampType)
      .add("ingest_date", DateType)

    val stream = spark.readStream
      .schema(txnSchema)
      .option("maxFilesPerTrigger", 2)
      .parquet("data/synthetic/transaction")

    // Stream-static joins: streaming side LEFT JOIN static side
    val enriched = stream
      .join(products, Seq("product_id"), "left")
      .join(branches, Seq("branch_id"), "left")
      .withColumn("product_name", coalesce(col("product_name"), lit("Unknown")))
      .withColumn("branch_name", coalesce(col("branch_name"), lit("Unknown")))

    val query = enriched.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "output/streaming/enriched")
      .option("checkpointLocation", "output/streaming/checkpoint_21")
      .start()

    query.awaitTermination(60000)
    query.stop()
    spark.stop()
  }
}
```

**Key takeaways:**
- **Stream-static join:** The static DataFrame is loaded once and held in driver memory. Each micro-batch joins against it without reshuffling.
- Spark automatically broadcasts the static side since it's small.
- You **cannot** join two streaming DataFrames with a simple join — you'd need watermarks on both sides and specific join types (inner, left outer with watermark on the right).
- No state management needed for stream-static joins — they're stateless.

---

### Solution 22: Streaming Deduplication

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Problem22_StreamingDedup {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem22")

    val txnSchema = new StructType()
      .add("transaction_id", LongType)
      .add("account_id", LongType)
      .add("amount", DoubleType)
      .add("event_ts", TimestampType)

    val stream = spark.readStream
      .schema(txnSchema)
      .option("maxFilesPerTrigger", 2)
      .parquet("data/synthetic/transaction")

    // Option A: Watermark-bounded dedup (SAFE — state is bounded)
    val dedupedBounded = stream
      .withWatermark("event_ts", "3 days")
      .dropDuplicatesWithinWatermark("transaction_id")

    // Option B: Unbounded dedup (DANGEROUS — state grows forever)
    // val dedupedUnbounded = stream.dropDuplicates("transaction_id")

    val query = dedupedBounded.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "output/streaming/deduped")
      .option("checkpointLocation", "output/streaming/checkpoint_22")
      .start()

    query.awaitTermination(60000)
    query.stop()
    spark.stop()
  }
}
```

**Key takeaways:**
- `dropDuplicates("transaction_id")` keeps **all** seen transaction IDs in the state store **forever**. With 50M unique IDs, this consumes GBs of memory and never shrinks.
- `dropDuplicatesWithinWatermark("transaction_id")` only keeps IDs within the watermark window (3 days). Once a window is evicted, those IDs are forgotten. If a duplicate arrives after 3 days, it'll pass through — acceptable trade-off.
- In production, always use the watermark-bounded version unless you have infinite memory.

---

### Solution 23: Streaming Data Quality with `foreachBatch`

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Problem23_StreamingQuality {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem23")

    val validCount = spark.sparkContext.longAccumulator("valid_total")
    val quarantineCount = spark.sparkContext.longAccumulator("quarantine_total")
    var batchCounter = 0L

    val txnSchema = new StructType()
      .add("transaction_id", LongType)
      .add("account_id", LongType)
      .add("amount", DoubleType)
      .add("currency", StringType)
      .add("transaction_type", StringType)
      .add("event_date", DateType)
      .add("event_ts", TimestampType)

    val stream = spark.readStream
      .schema(txnSchema)
      .option("maxFilesPerTrigger", 2)
      .parquet("data/synthetic/transaction")

    val query = stream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val isValid =
          col("transaction_id").isNotNull &&
          col("account_id").isNotNull && col("account_id") > 0 &&
          col("amount").isNotNull && col("amount") =!= -999999.99 &&
          col("currency").isNotNull && col("currency") =!= "XXX" &&
          col("transaction_type").isNotNull && col("transaction_type") =!= "UNKNOWN"

        val valid = batchDF.filter(isValid)
        val quarantine = batchDF.filter(!isValid)

        val vc = valid.count()
        val qc = quarantine.count()
        validCount.add(vc)
        quarantineCount.add(qc)

        valid.write.mode("append").parquet("output/streaming/valid")
        quarantine.write.mode("append").parquet("output/streaming/quarantine")

        batchCounter += 1
        if (batchCounter % 10 == 0) {
          println(s"\n=== Quality Summary (after $batchCounter batches) ===")
          println(s"Valid total:      ${validCount.value}")
          println(s"Quarantine total: ${quarantineCount.value}")
          println(s"Quarantine rate:  ${quarantineCount.value * 100.0 / (validCount.value + quarantineCount.value)}%")
        }
      }
      .option("checkpointLocation", "output/streaming/checkpoint_23")
      .start()

    query.awaitTermination(120000)
    query.stop()

    println(s"\nFinal — Valid: ${validCount.value}, Quarantine: ${quarantineCount.value}")
    spark.stop()
  }
}
```

**Key takeaways:**
- `foreachBatch` gives you a **regular DataFrame** per micro-batch — you can use all batch DataFrame API features (joins, aggregations, multiple writes).
- Each micro-batch can write to multiple sinks (valid + quarantine).
- Accumulators are updated across micro-batches and persist for the driver's lifetime.
- For idempotency on retries, use `batchId` to create unique output paths: `valid.write.mode("overwrite").parquet(s"output/streaming/valid/batch_$batchId")`.

---

### Solution 24: Streaming Dashboard — Complete Output Mode

```scala
package com.ledgerforge.pipeline.problems

import com.ledgerforge.pipeline.core.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Problem24_StreamingDashboard {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.getSparkSession("Problem24")

    val txnSchema = new StructType()
      .add("transaction_id", LongType)
      .add("amount", DoubleType)
      .add("currency", StringType)
      .add("transaction_type", StringType)
      .add("event_ts", TimestampType)

    val stream = spark.readStream
      .schema(txnSchema)
      .option("maxFilesPerTrigger", 2)
      .parquet("data/synthetic/transaction")

    // Running totals by type and currency
    val dashboard = stream
      .groupBy("transaction_type", "currency")
      .agg(
        sum("amount").as("total_amount"),
        count("*").as("volume")
      )

    // Write to in-memory table for interactive SQL queries
    val query = dashboard.writeStream
      .outputMode("complete") // rewrite entire result each micro-batch
      .format("memory")
      .queryName("dashboard")
      .start()

    // Query the in-memory table periodically
    for (_ <- 1 to 5) {
      Thread.sleep(10000) // wait 10 seconds between queries
      println("\n=== Dashboard Snapshot ===")
      spark.sql("SELECT * FROM dashboard ORDER BY total_amount DESC").show(20)
    }

    query.stop()
    spark.stop()
  }
}
```

**Key takeaways:**
- **`complete` mode** rewrites the entire result table each micro-batch. Only feasible when the result is small (here: 4 types × 5 currencies = 20 rows max).
- **`update` mode** would only emit rows that changed — more efficient for large result sets.
- **`append` mode** doesn't work with aggregations without watermarks (Spark doesn't know when a group is "final").
- The `memory` sink stores results in a Spark SQL table that you can query with `spark.sql(...)`. Great for debugging and dashboards.
- **`complete` mode does NOT support watermarks** — it keeps all data because it needs to recompute the full result.

---

## Streaming Output Modes Quick Reference

| Mode | What it Outputs | When to Use | Watermark Support |
|------|----------------|-------------|-------------------|
| **append** | Only new/finalized rows | Watermarked windowed aggregations, stream-static joins | Required for aggregations |
| **update** | Only changed rows | Aggregations where you want incremental updates | Optional (helps with state cleanup) |
| **complete** | Entire result table | Small result sets, dashboards | Not supported |

---

## Quick Reference: Spark Optimization Cheat Sheet (Updated)

| Technique | When to Use | Spark Config / API |
|-----------|-------------|-------------------|
| Broadcast join | Dimension < 10MB | `broadcast(df)` |
| Salting | Skewed join keys | Manual: add salt + explode |
| Bucketing | Repeated joins on same key | `.bucketBy().saveAsTable()` |
| AQE | Always in Spark 3.x | `spark.sql.adaptive.enabled=true` |
| Partition pruning | Partitioned Parquet + filter | `filter(col("event_date") > ...)` |
| Cache | Multi-use DataFrame | `.cache()` / `.persist(level)` |
| Checkpoint | Very long lineage | `.checkpoint()` |
| Repartition | Control parallelism/file count | `.repartition(n, col)` |
| Coalesce | Reduce partitions (no shuffle) | `.coalesce(n)` |
| Native expressions | Always (avoid UDFs) | `when/otherwise`, `expr()` |
| Dynamic partition overwrite | Incremental writes | `partitionOverwriteMode=dynamic` |
| Watermark | Streaming late data / state | `.withWatermark("col", "3 days")` |
| foreachBatch | Multi-sink streaming | `.writeStream.foreachBatch(...)` |
| Stream-static join | Enrich stream with dimension | `stream.join(staticDF, ...)` |

