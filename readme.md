# LedgerForge

LedgerForge is a **production-grade Data project** focused on solving real-world data engineering problems such as **data skew, deduplication, late-arriving data, SCD Type 2 dimensions, and performance trade-offs at scale**.

It simulates realistic banking datasets with millions of records and embedded data quality issues to force correct and scalable Spark design.

---

## 🎯 Project Goals

- Work with **large datasets (10M–50M+ rows)**
- Practice correctness-first data engineering
- Understand Spark execution plans, shuffles, and skew
- Explore trade-offs between performance and accuracy

---

## 🧱 Tech Stack

- **Apache Spark** (Batch)
- **Scala**
- **Parquet**
- **Local mode (Mac M1, 16GB RAM)**
- **IntelliJ IDEA**
- **Maven**

---

## 📊 Datasets

All datasets are **synthetically generated using Spark**, not static CSVs.

### `fact_transactions`
- ~50 million rows
- Partitioned by `event_date`
- Intentional issues:
    - duplicate transaction IDs
    - skewed account distribution
    - late-arriving data
- Stored as Parquet

### `dim_customer_scd2`
- Slowly Changing Dimension (Type 2)
- Multiple versions per customer
- Used for temporal joins

### `dim_account`
- Account → customer mapping
- Status lag and inconsistencies included

### `dim_product`
- Slowly changing attributes

### `dim_branch`
- Highly skewed distribution
- Realistic parsing and data quality issues

---

## 🧪 What This Project Intentionally Breaks

LedgerForge embeds **real production failure modes**, including:

- Duplicate facts requiring deduplication
- Late-arriving events that invalidate historical aggregates
- Skewed joins causing shuffle imbalance
- Memory-heavy aggregations (`countDistinct`)
- Malformed input records
- Ambiguous business logic

---

## 🧠 Core Learning Challenges

This project is designed to solve (not yet solved):

1. Deduplicate transactions correctly and efficiently
2. Compute daily customer spend with late data
3. Perform SCD2 temporal joins at scale
4. Handle skewed joins without brute-force repartitioning
5. Analyze Spark UI and execution plans
6. Balance correctness vs performance trade-offs

---

## ▶️ Running the Project

Example:

```bash
mvn clean package
java -cp target/ledger-forge-*.jar com.ledgerforge.pipeline.Main --env dev