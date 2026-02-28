package com.ledgerforge.pipeline.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class CustomerTransformer extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df
      // Standardise names to title-case and trim whitespace
      .withColumn("first_name", initcap(trim(col("first_name"))))
      .withColumn("last_name",  initcap(trim(col("last_name"))))

      // Derived: single display name
      .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

      // Derived: email domain (everything after '@')
      .withColumn("email_domain",
        when(col("email").isNotNull && col("email").contains("@"),
          element_at(split(col("email"), "@"), 2))
        .otherwise(lit(null).cast("string")))

      // Data quality flags — useful for downstream filtering or reporting
      .withColumn("is_email_valid",
        col("email").isNotNull &&
        col("email").rlike("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"))

      .withColumn("is_phone_valid",
        col("phone").isNotNull &&
        col("phone").rlike("^\\d{3}-\\d{3}-\\d{4}$"))

      // Audit column
      .withColumn("ingest_date", current_date())
  }
}