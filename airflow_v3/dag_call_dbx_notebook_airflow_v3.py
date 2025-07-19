# --------------------------------------------
# Imports - Airflow 3 Compatible
# --------------------------------------------

from airflow import DAG, Dataset  # DAG and Dataset tracking
from airflow.operators.python import PythonOperator  # For Python tasks
from airflow.operators.empty import EmptyOperator  # Replaces DummyOperator
from airflow.operators.email import EmailOperator  # Send success/failure notifications
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup  # Astro-Databricks plugin

from airflow.utils.timezone import datetime  # Timezone-safe datetime
from airflow.decorators import dag  # Decorator-style DAG
from datetime import timedelta
import os
import pendulum

# --------------------------------------------
# Basic Configuration and DAG Metadata
# --------------------------------------------

# Get the DAG ID from filename, or hardcode it for clarity
dag_id = "demo_pipeline_dag"

# Databricks connection ID defined in Airflow UI (Admin > Connections)
databricks_conn_id = "databricks_default"

# Environment info (can be used in notebook logic)
env = "dev"  # e.g., dev / staging / prod

# Start date for DAG scheduling (set to yesterday to avoid accidental backfill)
yesterday = datetime.now() - timedelta(days=1)
dag_start_date = pendulum.datetime(yesterday.year, yesterday.month, yesterday.day)

# Custom pipeline run ID (based on logical execution time)
pipeline_start_time = '{{ dag_run.logical_date.strftime("%Y%m%d_%H%M%S") }}'
run_id = f"af_{dag_id}_run_{pipeline_start_time}"  # Custom run name (e.g., af_demo_pipeline_dag_run_20250717_001500)

# --------------------------------------------
# Databricks Cluster Configuration
# --------------------------------------------

Cluster_id_1 = [
    {
        "job_cluster_key": "Cluster_id_1",  # Reference key used in tasks
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",  # Databricks Runtime version
            "node_type_id": "Standard_D8s_v3",  # Azure VM type
            "num_workers": 1,  # Number of workers in the cluster
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"  # Python environment
            },
            "cluster_log_conf": {
                "dbfs": {"destination": "dbfs:/mnt/cluster-logs"}  # Cluster logs path in DBFS
            },
            "init_scripts": [
                {"workspace": {"destination": "/Workspace/init-scripts/framework.sh"}},  # Sample init scripts
                {"workspace": {"destination": "/Workspace/init-scripts/master.sh"}}
            ],
        },
    }
]

# --------------------------------------------
# DAG Definition Using @dag Decorator
# --------------------------------------------

@dag(
    dag_id=dag_id,
    start_date=dag_start_date,
    schedule='15 23 * * 6',  # Runs every Saturday at 23:15
    catchup=False,
    max_active_runs=1,
    tags=['Scheduled', 'Databricks', 'Airflow3'],
    default_args={
        "owner": "data_engineering_team"
    }
)
def demo_pipeline_dag():

    # ----------------------------------------
    # Define Databricks Workflow Task Group
    # ----------------------------------------

    job_Cluster_id_1 = DatabricksWorkflowTaskGroup(
        group_id="Cluster_id_1_grp",  # Used as prefix for all tasks inside
        job_clusters=Cluster_id_1,  # Pass the cluster configuration defined above
        databricks_conn_id=databricks_conn_id,
        notebook_packages=[
            {"pypi": {"package": "pandas"}},  # Install pandas on the cluster
            {
                "maven": {
                    "coordinates": "uk.co.gresearch.spark:spark-extension_2.12:1.3.3-3.1"
                }
            },
        ],
    )

    # ----------------------------------------
    # Databricks Notebook Task Inside the Cluster Group
    # ----------------------------------------

    with job_Cluster_id_1:

        dbx_demo = DatabricksNotebookOperator(
            task_id="dbx_call_from_airflow",  # Task name in the Airflow UI
            job_cluster_key="Cluster_id_1",  # Connect this task to the above cluster
            notebook_path="/projects/demo_project/demo_notebook",  # Path to your notebook in the Databricks workspace
            notebook_params={
                "pipeline_id": run_id,  # Custom run ID
                "env": env,
                "pipeline_name": dag_id,
                "pipeline_dag_run_id": "{{ dag_run.run_id }}",  # Built-in run_id from Airflow context
                "MetricDesc": "Demo notebook called from Airflow",
                "lowerBoundary": "100000",  # Optional parameter used inside the notebook
                "upperBoundary": "200000",  # Optional parameter used inside the notebook
                "Severity": "Medium",  # Optional parameter used inside the notebook
                "trace_partition_timestamp": "{{ dag_run.logical_date.strftime('%Y%m%d%H%M%S') }}"  # Trace timestamp
            },
            databricks_conn_id=databricks_conn_id,
            source="WORKSPACE",
            outlets=[Dataset("demo.demo_dataset")]  # Marks this dataset as updated
        )

    # ----------------------------------------
    # Final Task - Marks End of DAG Execution
    # ----------------------------------------

    final_status = EmptyOperator(
        task_id="final_status"
    )

    # ----------------------------------------
    # Exception Handling - Triggered on Any Failure
    # ----------------------------------------

    exception_handling = PythonOperator(
        task_id="exception_handling",
        python_callable=lambda: print("Trigger pager duty or alert system"),
        trigger_rule="one_failed"  # Only runs if previous task fails
    )

    # ----------------------------------------
    # Success Email Notification
    # ----------------------------------------

    email_success = EmailOperator(
        task_id="email_on_success",
        to="team@example.com",
        subject=f"Airflow DAG {dag_id} Succeeded",
        html_content=f"The DAG <b>{dag_id}</b> completed successfully at {{ ts }}.",
        trigger_rule="all_success"
    )

    # ----------------------------------------
    # Failure Email Notification
    # ----------------------------------------

    email_failure = EmailOperator(
        task_id="email_on_failure",
        to="team@example.com",
        subject=f"Airflow DAG {dag_id} Failed",
        html_content=f"The DAG <b>{dag_id}</b> failed at {{ ts }}. Please investigate.",
        trigger_rule="one_failed"
    )

    # ----------------------------------------
    # DAG Task Dependencies
    # ----------------------------------------

    dbx_demo >> final_status >> email_success        # Normal success path
    dbx_demo >> exception_handling >> email_failure  # Failure path

# --------------------------------------------
# Register the DAG
# --------------------------------------------

demo_pipeline_dag()
