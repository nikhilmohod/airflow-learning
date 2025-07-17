'''Import Modules'''
import os
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup
from datetime import datetime, timedelta
import pendulum
from airflow_functions import *

'''Default Arguments'''
dag_id = os.path.basename(__file__).split(".")[0]
dag_run_id = '{{ dag_run.run_id }}'
databricks_conn_id = "databricks_default"
env = "dev"
email_url = Variable.get('email_notification_logicapp')
default_init_framework = Variable.get('default_init_framework')
default_init_master = Variable.get('default_init_master')
cluster_log_destination = Variable.get('cluster_log_destination')

yesterday_dag_date = datetime.now() - timedelta(days=1)
dag_start_date = pendulum.datetime(yesterday_dag_date.year, yesterday_dag_date.month, yesterday_dag_date.day)

pipeline_start_time = '{{ dag_run.start_date.strftime("%Y%m%d_%H%M%S") }}'
RunId = 'af_' + dag_id + '_run_' + str(pipeline_start_time)

'''Cluster Configurations'''
Cluster_id_1 = [
    {
        "job_cluster_key": "Cluster_id_1",
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "Standard_D8s_v3",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "num_workers": "1",
            "cluster_log_conf": {"dbfs": {"destination": cluster_log_destination}},
            "init_scripts": [
                {"workspace": {"destination": f"{default_init_framework}"}},
                {"workspace": {"destination": f"{default_init_master}"}}
            ],
        }
    }
]

'''DAG Definition'''
with DAG(
    dag_id=dag_id,
    start_date=dag_start_date,
    tags=['Scheduled', '11.3.x-scala2.12', 'DBX'],
    schedule='15 23 * * 6',
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": "DEMOOWNER",
        "default_view": "graph",
        'dag_run_id': dag_run_id,
    },
) as dag:

    # Define task group using Databricks workflow
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
        '''Databricks Notebook Task'''
        dbx_demo = DatabricksNotebookOperator(
            job_cluster_key="Cluster_id_1",
            task_id="dbx_call_from_airflow",
            notebook_path="/projects/demo_project/demo_notebook",
            notebook_params={
                'pipeline_id': RunId,
                'env': env,
                'pipeline_name': dag_id,
                'pipeline_dag_run_id': dag_run_id,
                'MetricDesc': 'Demo notebook called from airflow',
                'lowerBoundary': '100000',
                'upperBoundary': '200000',
                'Severity': 'Medium',
                "trace_partition_timestamp": "{{ ts_nodash }}"
            },
            databricks_conn_id=databricks_conn_id,
            source="WORKSPACE",
            outlets=[Dataset('demo.demo_dataset')]
        )

    '''Success Email'''
    email_on_success = EmailOperator(
        task_id='send_success_email',
        to='team@example.com',
        subject=f"[Airflow] DAG {dag_id} Succeeded",
        html_content=f"DAG {dag_id} completed successfully at {{ ts }}."
    )

    '''Failure Email'''
    email_on_failure = EmailOperator(
        task_id='send_failure_email',
        to='team@example.com',
        subject=f"[Airflow] DAG {dag_id} Failed",
        html_content=f"DAG {dag_id} failed at {{ ts }}.",
        trigger_rule='one_failed'
    )

    '''Exception Handling'''
    exception_handling = PythonOperator(
        task_id='exception_handling',
        trigger_rule='one_failed',
        python_callable=raise_pagerduty_inc,
        op_kwargs={"dag_id": dag_id, "Impact": "demo notebook failed"},
    )

    '''Final Dummy Task'''
    final_status = DummyOperator(
        task_id='final_status',
    )

    '''DAG Dependencies'''
    dbx_demo >> final_status >> email_on_success
    dbx_demo >> exception_handling >> email_on_failure
