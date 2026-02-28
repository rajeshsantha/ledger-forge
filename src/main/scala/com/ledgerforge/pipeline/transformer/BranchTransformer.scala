package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class BranchTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df
      // Normalise branch_name to upper-case (kept for downstream compatibility)
      .withColumn("branch_upper", upper(col("branch_name")))

      // Trim whitespace from city and state — CSV data has " State 2" with leading space
      .withColumn("city",  trim(col("city")))
      .withColumn("state", trim(col("state")))

      // Extract branch type from name: "Main Branch 1" → "Main", "Downtown Branch 2" → "Downtown"
      .withColumn("branch_type",
        when(lower(col("branch_name")).contains("main"),         lit("Main"))
        .when(lower(col("branch_name")).contains("downtown"),    lit("Downtown"))
        .when(lower(col("branch_name")).contains("suburban"),    lit("Suburban"))
        .when(lower(col("branch_name")).contains("airport"),     lit("Airport"))
        .when(lower(col("branch_name")).contains("online"),      lit("Online"))
        .when(lower(col("branch_name")).contains("international"),lit("International"))
        .otherwise(lit("Other")))

      // Flag data quality: state should be a non-null 2-letter code
      .withColumn("is_state_valid",
        col("state").isNotNull && col("state").rlike("^[A-Z]{2}$"))

      .withColumn("ingest_date", current_date())
  }
}
