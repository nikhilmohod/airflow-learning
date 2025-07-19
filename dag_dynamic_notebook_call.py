# --------------------------------------------
# Import Required Modules
# --------------------------------------------

# Airflow core components
from airflow import DAG
from airflow.models import Variable

# Operators
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# Databricks-specific operators (from Astro Databricks provider)
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup

# Python modules
from datetime import datetime, timedelta
import pendulum


# --------------------------------------------
# DAG-Level Constants and Configuration
# --------------------------------------------

dag_id = "demo_adhoc_databricks_dag"  # Unique DAG ID
databricks_conn_id = "databricks_uc"  # Databricks connection configured in Airflow UI
env = "dev"                            # Example environment value
uc_policy_id = "policy-abc123"        # Example Unity Catalog policy ID

# Retry behavior for the notebook task
retry_count = 2                       # Number of retry attempts
retry_delay_minutes = 5              # Delay (in minutes) between retries

# DAG start date set to "yesterday" to avoid scheduling conflicts
dag_start_date = pendulum.datetime(
    datetime.now().year,
    datetime.now().month,
    datetime.now().day - 1
)


# --------------------------------------------
# Databricks Cluster Configuration
# --------------------------------------------

# This defines the compute cluster that Databricks will use to run the notebook.
Cluster_id_1 = [
    {
        "job_cluster_key": "Cluster_id_1",  # Logical cluster name used by notebook task
        "new_cluster": {
            "spark_version": "14.3.x-scala2.12",     # Databricks runtime version
            "node_type_id": "Standard_D16s_v3",       # VM instance type
            "num_workers": 10,                        # Number of worker nodes
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"  # Use Python 3
            },
            "cluster_log_conf": {
                "dbfs": {
                    "destination": "dbfs:/mnt/cluster-logs"  # Where logs are stored
                }
            },
            "custom_tags": {
                "DBUClusterName": "pass_parameter_toDBX_cluster_1"  # Tag for tracking
            },
            "policy_id": uc_policy_id  # Enforce Unity Catalog policy
        }
    }
]


# --------------------------------------------
# DAG Definition
# --------------------------------------------

with DAG(
    dag_id=dag_id,
    start_date=dag_start_date,
    schedule=None,               # No automatic scheduling; triggered manually
    max_active_runs=1,           # Only one DAG instance allowed at a time
    catchup=False,               # Skip missed DAG runs
    tags=['Adhoc', 'Databricks'],  # Tags for UI filtering
    default_args={
        "owner": "DEMOOWNER"
    }
) as dag:

    # ----------------------------------------
    # Define a Databricks Task Group (Cluster Wrapper)
    # ----------------------------------------

    job_Cluster_id_1 = DatabricksWorkflowTaskGroup(
        group_id="Cluster_id_1_grp",  # Logical name for the group
        job_clusters=Cluster_id_1,    # Cluster config defined above
        databricks_conn_id=databricks_conn_id,  # Connection ID for Databricks
        notebook_packages=[
            {"pypi": {"package": "pandas"}}  # Example PyPI dependency
        ]
    )

    # All tasks in this block will use the above cluster
    with job_Cluster_id_1:

        # ------------------------------------
        # Databricks Notebook Task
        # ------------------------------------

        dbx_adhoc_task = DatabricksNotebookOperator(
            task_id="dbx_adhoc_task",  # Unique task ID within the DAG
            job_cluster_key="Cluster_id_1",  # Use the cluster defined earlier
            notebook_path="/Workspace/projects/adhoc/pass_parameter_to_notebook",  # Notebook path in DBX workspace
            notebook_params={  # Parameters to be passed into the notebook
                "dag_id": dag_id,
                "dag_run_id": "manual__2025-07-17T09:15:00",  # Example run ID
                "dbx_notebook_path": "/Workspace/projects/adhoc/pass_parameter_to_notebook",
                "dbx_parameters": "--input data.csv",
                "dag_group_id": "grp_1",
                "dag_task_id": "adhoc_runner_task",
                "pipeline_id": "af_demo_adhoc_databricks_dag_run_20250717_091500",
                "pipeline_name": "Demo_Pipeline"
            },
            databricks_conn_id=databricks_conn_id,
            source="WORKSPACE",
            retries=retry_count,
            retry_delay=timedelta(minutes=retry_delay_minutes),
            trigger_rule='all_done'  # Ensures task completes regardless of prior success/failure
        )

    # ----------------------------------------
    # Dummy Task to Represent Dataset Output
    # ----------------------------------------
    outlet_activity = DummyOperator(
        task_id='outlet_activity'  # Could be used to signal a dataset was updated
    )

    # ----------------------------------------
    # Email Notification on Success
    # ----------------------------------------
    email_on_success = EmailOperator(
        task_id='send_success_email',
        to='team@example.com',
        subject="Airflow DAG demo_adhoc_databricks_dag Succeeded",
        html_content="DAG demo_adhoc_databricks_dag completed successfully at 2025-07-17 09:15:00."
    )

    # ----------------------------------------
    # Email Notification on Failure
    # ----------------------------------------
    email_on_failure = EmailOperator(
        task_id='send_failure_email',
        to='team@example.com',
        subject="Airflow DAG demo_adhoc_databricks_dag Failed",
        html_content="DAG demo_adhoc_databricks_dag failed at 2025-07-17 09:15:00.",
        trigger_rule='one_failed'  # Only send this if a task failed
    )

    # ----------------------------------------
    # Optional Exception Handling
    # ----------------------------------------
    exception_handling = PythonOperator(
        task_id='exception_handling',
        python_callable=lambda: print("Triggering PagerDuty or Alert Logic here."),  # Replace with real logic
        trigger_rule='one_failed'  # Runs only if something fails upstream
    )


    # ----------------------------------------
    # Set Task Dependencies (Execution Order)
    # ----------------------------------------

    # Success path: If notebook completes, mark output and send success email
    job_Cluster_id_1 >> outlet_activity >> email_on_success

    # Failure path: If notebook fails, trigger exception handler and send failure email
    job_Cluster_id_1 >> exception_handling >> email_on_failure
