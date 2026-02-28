package com.ledgerforge.pipeline

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Base trait for all Spark unit tests.
 *
 * HOW IT WORKS:
 * - Creates ONE SparkSession shared across ALL tests in a suite (BeforeAndAfterAll).
 * - Uses local[1] — single thread, deterministic, no port conflicts.
 * - Disables the Spark UI to avoid port binding issues in CI.
 * - Stops Spark once all tests in the suite finish.
 *
 * EXTEND THIS in every test class:
 *   class MyTransformerSpec extends SparkTestBase { ... }
 */
trait SparkTestBase extends AnyFunSuite with BeforeAndAfterAll {

  // 'implicit' makes `spark` available automatically in import spark.implicits._
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[1]")          // single thread — deterministic test output
      .appName(this.getClass.getSimpleName)
      .config("spark.ui.enabled", "false")           // no Spark UI in tests
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")   // avoid 200-partition overhead in tests
      .config("spark.sql.adaptive.enabled", "false") // predictable plans for assertions
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // suppress INFO/WARN noise in test output
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }
}

