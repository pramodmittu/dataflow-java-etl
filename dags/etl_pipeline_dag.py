"""
DAG: etl_pipeline
Task 1: Dataflow — data.txt → BQ stage_table
Task 2: BQ copy — stage_table → stage_table_copy
Task 3: Create Dataproc cluster
Task 4: PySpark — BQ stage_table_copy → MySQL
Task 4b: Delete cluster (always runs)
"""

import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

config      = json.loads(Variable.get("pipeline_config"))
PROJECT     = config["project_id"]
REGION      = config["region"]
ZONE        = config["zone"]
BUCKET      = config["gcs"]["bucket"]
JAR_PATH    = config["dataflow"]["jar_path"]
KMS_KEY     = config["dataflow"]["kms_key"]
INPUT_FILE  = config["gcs"]["input_file"]
DATASET     = config["bigquery"]["dataset"]
TABLE_1     = config["bigquery"]["table_1"]
TABLE_2     = config["bigquery"]["table_2"]
CLUSTER     = config["dataproc"]["cluster_name"]
SPARK_SCRIPT= config["gcs"]["spark_script"]
MYSQL_HOST  = config["mysql"]["host"]
MYSQL_DB    = config["mysql"]["database"]
MYSQL_TABLE = config["mysql"]["table"]
MYSQL_USER  = config["mysql"]["user"]

default_args = {
    "owner"           : "pramod",
    "start_date"      : datetime(2026, 1, 1),
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id            = "etl_pipeline",
    description       = "GCS → BQ → BQ → Dataproc → MySQL",
    default_args      = default_args,
    schedule_interval = None,
    catchup           = False,
    tags              = ["etl", "dataflow", "dataproc", "mysql"],
) as dag:

    run_dataflow = DataflowCreateJavaJobOperator(
        task_id   = "task1_dataflow_gcs_to_bq",
        jar       = JAR_PATH,
        job_name  = "etl-gcs-to-bq-{{ ds_nodash }}",
        options   = {
            "runner"         : "DataflowRunner",
            "project"        : PROJECT,
            "region"         : REGION,
            "stagingLocation": f"gs://{BUCKET}/dataflow-staging",
            "tempLocation"   : f"gs://{BUCKET}/tmp",
            "inputFile"      : INPUT_FILE,
            "outputTable"    : f"{PROJECT}:{DATASET}.{TABLE_1}",
            "kmsKey"         : KMS_KEY,
        },
        location    = REGION,
        gcp_conn_id = "google_cloud_default",
        poll_sleep  = 30,
    )

    bq_to_bq_copy = BigQueryInsertJobOperator(
        task_id = "task2_bq_table1_to_table2",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.{TABLE_2}` AS
                    SELECT *, CURRENT_TIMESTAMP() AS copied_at
                    FROM `{PROJECT}.{DATASET}.{TABLE_1}`
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id = "google_cloud_default",
        location    = "US",
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id      = "task3_create_dataproc_cluster",
        project_id   = PROJECT,
        region       = REGION,
        cluster_name = CLUSTER,
        cluster_config = {
            "master_config": {
                "num_instances"  : 1,
                "machine_type_uri": config["dataproc"]["master_machine"],
                "disk_config"    : {"boot_disk_size_gb": 50},
            },
            "worker_config": {
                "num_instances"  : config["dataproc"]["num_workers"],
                "machine_type_uri": config["dataproc"]["worker_machine"],
                "disk_config"    : {"boot_disk_size_gb": 50},
            },
            "software_config": {
                "image_version": config["dataproc"]["image_version"],
                "properties"   : {"dataproc:pip.packages": "mysql-connector-python==8.0.33"},
            },
            "gce_cluster_config": {"zone_uri": ZONE},
        },
        gcp_conn_id = "google_cloud_default",
    )

    submit_spark = DataprocSubmitJobOperator(
        task_id    = "task4_spark_bq_to_mysql",
        project_id = PROJECT,
        region     = REGION,
        job = {
            "placement"  : {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": SPARK_SCRIPT,
                "args": [
                    f"--project={PROJECT}",
                    f"--bq_dataset={DATASET}",
                    f"--bq_table={TABLE_2}",
                    f"--mysql_host={MYSQL_HOST}",
                    f"--mysql_db={MYSQL_DB}",
                    f"--mysql_table={MYSQL_TABLE}",
                    f"--mysql_user={MYSQL_USER}",
                    "--mysql_password_secret=mysql-password",
                    f"--temp_gcs_bucket=gs://{BUCKET}/tmp/spark/",
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar"
                ],
            },
        },
        gcp_conn_id = "google_cloud_default",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id      = "task4b_delete_dataproc_cluster",
        project_id   = PROJECT,
        region       = REGION,
        cluster_name = CLUSTER,
        gcp_conn_id  = "google_cloud_default",
        trigger_rule = TriggerRule.ALL_DONE,
    )

    run_dataflow >> bq_to_bq_copy >> create_cluster >> submit_spark >> delete_cluster
