"""
LedgerForge Daily Pipeline — Airflow DAG

Schedule: Daily at 2:00 AM
Runs spark-submit on the local machine to execute the LedgerForge ETL pipeline.

Setup (macOS):
  1. Install Airflow:
       pip install "apache-airflow[celery]==2.8.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.10.txt"

  2. Initialize the database:
       airflow db init

  3. Create an admin user:
       airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

  4. Copy this file to your Airflow DAGs folder:
       cp dags/ledgerforge_daily.py ~/airflow/dags/

  5. Start Airflow (two terminals):
       Terminal 1: airflow webserver --port 8080
       Terminal 2: airflow scheduler

  6. Open http://localhost:8080 and enable the 'ledgerforge_daily_pipeline' DAG.

Prerequisites:
  - Java 8+ installed (JAVA_HOME set)
  - Apache Spark installed locally (SPARK_HOME set), OR run via mvn exec:java
  - Project built: cd /path/to/LedgerForge && mvn package -DskipTests
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ---- Configuration ----
PROJECT_DIR = "/Users/rajeshsantha/IdeaProjects/LedgerForge"
JAR_PATH = f"{PROJECT_DIR}/target/ledger-forge-1.0-SNAPSHOT.jar"
MAIN_CLASS = "com.ledgerforge.pipeline.Main"

# If you have spark-submit on PATH:
SPARK_SUBMIT = "spark-submit"
# Otherwise, use full path:
# SPARK_SUBMIT = "/usr/local/opt/apache-spark/bin/spark-submit"

default_args = {
    "owner": "rajesh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ledgerforge_daily_pipeline",
    default_args=default_args,
    description="LedgerForge Spark ETL — daily batch pipeline",
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=["ledgerforge", "spark", "etl"],
) as dag:

    # Task 1: Build the project (compile + package)
    build = BashOperator(
        task_id="maven_build",
        bash_command=f"cd {PROJECT_DIR} && mvn package -DskipTests -q",
    )

    # Task 2: Generate synthetic data (optional — run only if data doesn't exist)
    generate_data = BashOperator(
        task_id="generate_synthetic_data",
        bash_command=f"""
            if [ ! -d "{PROJECT_DIR}/data/synthetic/transaction" ]; then
                cd {PROJECT_DIR} && {SPARK_SUBMIT} \
                    --class com.ledgerforge.pipeline.utils.DataGenerator \
                    --master "local[*]" \
                    --driver-memory 4g \
                    {JAR_PATH} \
                    numTransactions=1000000 \
                    numCustomers=100000 \
                    numAccounts=100000 \
                    numProducts=500 \
                    numBranches=50
            else
                echo "Synthetic data already exists, skipping generation."
            fi
        """,
    )

    # Task 3: Run the main ETL pipeline
    run_pipeline = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=f"""
            cd {PROJECT_DIR} && {SPARK_SUBMIT} \
                --class {MAIN_CLASS} \
                --master "local[*]" \
                --driver-memory 4g \
                --conf spark.sql.adaptive.enabled=true \
                --conf spark.sql.shuffle.partitions=200 \
                --conf spark.driver.bindAddress=127.0.0.1 \
                --conf spark.ui.enabled=true \
                {JAR_PATH} \
                --env dev
        """,
    )

    # Task 4: Run data quality check (Problem 8 from problem_statements.md)
    # Uncomment when you implement Problem08_DataQuality
    # quality_check = BashOperator(
    #     task_id="data_quality_check",
    #     bash_command=f"""
    #         cd {PROJECT_DIR} && {SPARK_SUBMIT} \
    #             --class com.ledgerforge.pipeline.problems.Problem08_DataQuality \
    #             --master "local[*]" \
    #             --driver-memory 4g \
    #             {JAR_PATH}
    #     """,
    # )

    # DAG dependencies
    build >> generate_data >> run_pipeline

