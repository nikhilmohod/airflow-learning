# Import required modules
import os
from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup

# --------------------------------------------
# DAG Configuration
# --------------------------------------------

# Unique name for the DAG
dag_id = "demo_databricks_dag"

# Databricks connection (configured in Airflow Connections)
databricks_conn_id = "databricks_default"

# Variables
env = "dev"
cluster_log_destination = "dbfs:/cluster-logs/"  # Where cluster logs will be stored
default_init_framework = "/Workspace/init-scripts/framework-init.sh"
default_init_master = "/Workspace/init-scripts/master-init.sh"

# Start date for the DAG (yesterday)
dag_start_date = pendulum.datetime(
    datetime.now().year, datetime.now().month, datetime.now().day - 1
)

# --------------------------------------------
# Databricks Cluster Configuration
# --------------------------------------------
Cluster_id_1 = [
    {
        "job_cluster_key": "Cluster_id_1",
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "Standard_D8s_v3",  # Azure node type
            "num_workers": 1,  # Single worker node
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
            "cluster_log_conf": {
                "dbfs": {
                    "destination": cluster_log_destination
                }
            },
            "init_scripts": [
                {"workspace": {"destination": default_init_framework}},
                {"workspace": {"destination": default_init_master}},
            ],
        }
    }
]

# --------------------------------------------
# DAG Definition
# --------------------------------------------
with DAG(
    dag_id=dag_id,
    start_date=dag_start_date,
    schedule='15 23 * * 6',  # Runs every Saturday at 11:15 PM
    max_active_runs=1,
    catchup=False,
    tags=['Scheduled', 'Databricks'],
    default_args={
        "owner": "demo_owner"
    }
) as dag:

    # ----------------------------------------
    # Databricks Workflow Task Group
    # ----------------------------------------
    job_Cluster_id_1 = DatabricksWorkflowTaskGroup(
        group_id="Cluster_id_1_grp",
        job_clusters=Cluster_id_1,
        databricks_conn_id=databricks_conn_id,
        notebook_packages=[
            {"pypi": {"package": "pandas"}},
            {"maven": {"coordinates": "uk.co.gresearch.spark:spark-extension_2.12:1.3.3-3.1"}}
        ]
    )

    with job_Cluster_id_1:

        # ------------------------------------
        # Databricks Notebook Task
        # ------------------------------------
        dbx_demo = DatabricksNotebookOperator(
            task_id="dbx_call_from_airflow",
            job_cluster_key="Cluster_id_1",
            notebook_path="/projects/demo_project/demo_notebook",  # Databricks notebook path
            notebook_params={
                'pipeline_id': 'af_demo_run_20250717_091500',  # Example run ID
                'env': env,
                'pipeline_name': dag_id,
                'pipeline_dag_run_id': 'manual__2025-07-17T09:15:00',
                'MetricDesc': 'Demo notebook called from airflow',
                'lowerBoundary': '100000',
                'upperBoundary': '200000',
                'Severity': 'Medium',
                'trace_partition_timestamp': '20250717T091500'  # Static timestamp example
            },
            databricks_conn_id=databricks_conn_id,
            source="WORKSPACE"
        )

    # ----------------------------------------
    # Success Notification via Email
    # ----------------------------------------
    email_on_success = EmailOperator(
        task_id='send_success_email',
        to='team@example.com',
        subject="Airflow DAG demo_databricks_dag Succeeded",
        html_content="DAG demo_databricks_dag completed successfully at 2025-07-17 09:15:00."
    )

    # ----------------------------------------
    # Failure Notification via Email
    # ----------------------------------------
    email_on_failure = EmailOperator(
        task_id='send_failure_email',
        to='team@example.com',
        subject="Airflow DAG demo_databricks_dag Failed",
        html_content="DAG demo_databricks_dag failed at 2025-07-17 09:15:00.",
        trigger_rule='one_failed'  # Trigger this only if a previous task fails
    )

    # ----------------------------------------
    # Dummy Task for Final Step (Success Path)
    # ----------------------------------------
    final_status = DummyOperator(
        task_id='final_status'
    )

    # ----------------------------------------
    # Task Dependencies
    # ----------------------------------------
    dbx_demo >> final_status >> email_on_success  # Success flow
    dbx_demo >> email_on_failure  # Failure flow

