''' Updated Imports for Airflow 3 '''
from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator  # DummyOperator replaced with EmptyOperator
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup
from airflow.utils.timezone import datetime
from airflow.decorators import dag
import pendulum
import os
from datetime import datetime, timedelta
from airflow_functions import *

''' Default Arguments '''
dag_id = os.path.basename(__file__).split(".")[0]
dag_run_id = '{{ dag_run.run_id }}'
databricks_conn_id = "databricks_default"
env = Variable.get("env")
default_init_framework = Variable.get("default_init_framework")
default_init_master = Variable.get("default_init_master")

yesterday_dag_date = datetime.now() - timedelta(days=1)
dag_start_date = pendulum.datetime(
    yesterday_dag_date.year, yesterday_dag_date.month, yesterday_dag_date.day
)

# switched to logical_date for templating timestamps
pipeline_start_time = '{{ dag_run.logical_date.strftime("%Y%m%d_%H%M%S") }}'
RunId = 'af_' + dag_id + '_run_' + str(pipeline_start_time)
pipeline_start_time_invoke_orch_YmdHMS = '{{ dag_run.logical_date.strftime("%Y-%m-%d %H:%M:%S") }}'
pipeline_start_time_invoke_orch_mdYHMS = '{{ dag_run.logical_date.strftime("%m/%d/%Y %H:%M:%S") }}'
pipeline_end_time_invoke_orch_YmdHMS = pl_invoke_orch_current_time_YmdHMS()
pipeline_end_time_invoke_orch_mdYHMS = pl_invoke_orch_current_time_mdYHMS()

''' Cluster Configurations '''
Cluster_id_1 = [
    {
        "job_cluster_key": "Cluster_id_1",
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "Standard_D8s_v3",
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
            "num_workers": "1",
            "cluster_log_conf": {
                "dbfs": {"destination": "dbfs:/mnt/cluster-logs"}
            },
            "init_scripts": [
                {"workspace": {"destination": f"{default_init_framework}"}},
                {"workspace": {"destination": f"{default_init_master}"}}
            ],
        },
    }
]

# DAG Definition using @dag decorator instead of with DAG(...)
@dag(
    dag_id=dag_id,
    start_date=dag_start_date,
    tags=['Scheduled', '11.3.x-scala2.12', 'DBX'],
    schedule='15 23 * * 6',  # The DAG is scheduled to run every Saturday at 23:15
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": "DEMOOWNER",
        "default_view": "graph",
        "dag_run_id": dag_run_id,
    }
)
def demo_pipeline_dag():  # New DAG function definition

    job_Cluster_id_1 = DatabricksWorkflowTaskGroup(
        group_id="Cluster_id_1_grp",
        job_clusters=Cluster_id_1,
        databricks_conn_id=databricks_conn_id,
        notebook_packages=[
            {"pypi": {"package": "pandas"}},
            {
                "maven": {
                    "coordinates": "uk.co.gresearch.spark:spark-extension_2.12:1.3.3-3.1"
                }
            },
        ],
    )

    with job_Cluster_id_1:
        ''' Calling Databricks Notebook '''
        dbx_demo = DatabricksNotebookOperator(
            job_cluster_key="Cluster_id_1",
            task_id="dbx_call_from_airflow",
            notebook_path="/projects/demo_project/demo_notebook",
            notebook_params={
                "pipeline_id": RunId,
                "env": env,
                "pipeline_name": dag_id,
                "pipeline_dag_run_id": dag_run_id,
                "MetricDesc": "Demo notebook called from airflow",
                "lowerBoundary": "100000",  # Minimum threshold value for triggering logic or alert (e.g., metric lower limit)
                "upperBoundary": "200000",  # Maximum threshold value for triggering logic or alert (e.g., metric upper limit)
                "Severity": "Medium",	    # Severity level of the alert or condition (e.g., Low, Medium, High)    
                "trace_partition_timestamp": f"{concat(substring(utcnow(),0,4),substring(utcnow(),5,2),substring(utcnow(),8,2),substring(utcnow(),11,2),substring(utcnow(),14,2),substring(utcnow(),17,2))}"
            },
            databricks_conn_id=databricks_conn_id,
            source="WORKSPACE",
            outlets=[Dataset(f'demo.demo_dataset')],
        )

    ''' Exception Handling '''
    exception_handling = PythonOperator(
        task_id="exception_handling",
        trigger_rule="one_failed",
        python_callable=raise_pagerduty_inc,
        op_kwargs={"dag_id": dag_id, "Impact": f"demo notebook failed"},
    )

    ''' Final Status Update '''
    final_status = EmptyOperator(  # DummyOperator → EmptyOperator
        task_id="final_status"
    )

    ''' DAG Dependencies '''
    dbx_demo >> final_status
    dbx_demo >> exception_handling

''' DAG registration '''
demo_pipeline_dag()
