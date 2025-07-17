'''Import Modules''' 
from airflow.utils.timezone import datetime
from airflow import DAG
from airflow.decorators import dag, task_group
from airflow import Dataset
from airflow.models import Variable  
from airflow.operators.python_operator import PythonOperator
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup
from airflow.operators.dummy_operator import DummyOperator
import os
from datetime import datetime, timedelta
import pendulum
from airflow_functions import *

'''Default Arguments'''
dag_id = os.path.basename(__file__).split(".")[0]	# Dynamically set DAG ID from filename
dag_run_id = '{{ dag_run.run_id }}'			# Jinja template to pull DAG run ID from context
databricks_conn_id = "databricks_uc"
env = Variable.get("env")
email_url = Variable.get('email_notification_logicapp')
default_init_framework = Variable.get('default_init_framework')
default_init_master = Variable.get('default_init_master')
cluster_log_destination = Variable.get('cluster_log_destination')
retry_count_var = int(Variable.get("retry_count"))
retry_delay_var = int(Variable.get("retry_delay"))
yesterday_dag_date = datetime.now() - timedelta(days=1)
dag_start_date = pendulum.datetime(yesterday_dag_date.year, yesterday_dag_date.month, yesterday_dag_date.day)
uc_policy_id = Variable.get('uc_single_policy_id')

pipeline_start_time = '{{ dag_run.start_date.strftime("%Y%m%d_%H%M%S") }}'
RunId = 'af_' + dag_id + '_run_' + str(pipeline_start_time)
pipeline_start_time_invoke_orch_YmdHMS = '{{ dag_run.start_date.strftime("%Y-%m-%d %H:%M:%S") }}'
pipeline_start_time_invoke_orch_mdYHMS = '{{ dag_run.start_date.strftime("%m/%d/%Y %H:%M:%S") }}'
pipeline_end_time_invoke_orch_YmdHMS = pl_invoke_orch_current_time_YmdHMS()
pipeline_end_time_invoke_orch_mdYHMS = pl_invoke_orch_current_time_mdYHMS()

'''Cluster Configuration''' 
Cluster_id_1 = [
    {
        "job_cluster_key": "Cluster_id_1",
        "new_cluster": {
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "Standard_D16s_v3",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "num_workers": "10",
            "cluster_log_conf": {"dbfs": {"destination": "dbfs:/mnt/cluster-logs"}},
            "custom_tags": {"DBUClusterName": "pass_parameter_toDBX_cluster_1"},
            "policy_id": f"{uc_policy_id}"      # Enforce UC policy on the cluster
        }
    }
]

'''DAG Definition'''
dag_run_id, RunId, pipeline_start_time, dag_run_conf = get_dag_run_attributes(dag_id)
pipeline_id = RunId

with DAG(
    dag_id=dag_id,
    start_date=dag_start_date,
    tags=['Dataset', '14.3.x-scala2.12', 'DBX'],
    schedule=None,
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": "DEMOOWNER",
        "default_view": "graph",
        'dag_run_id': dag_run_id,
    },
    params={
        "dbx_notebook_path": '',
        "dbx_parameters": ''
    }
) as dag:

    job_Cluster_id_1 = DatabricksWorkflowTaskGroup(
        group_id="Cluster_id_1_grp",
        job_clusters=Cluster_id_1,
        databricks_conn_id=databricks_conn_id,
        notebook_packages=[{"pypi": {"package": "pandas"}}]
    )
    
    with job_Cluster_id_1:
        dbx_adhoc_task = DatabricksNotebookOperator(
            job_cluster_key="Cluster_id_1",
            task_id="dbx_adhoc_task",
            retry_delay=retry_delay_var,
            retries=retry_count_var,
            trigger_rule='all_done',
            notebook_path='/Workspace/projects/adhoc/pass_parameter_to_notebook',
            notebook_params={
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "dbx_notebook_path": dag_run_conf.get('dbx_notebook_path', ''),
                "dbx_parameters": dag_run_conf.get('dbx_parameters', ''),
                "dag_group_id": "grp_1",
                "dag_task_id": "adhoc_runner_task",
                "pipeline_id": RunId,
                "pipeline_name": "Demo_Pipeline"
            },
            databricks_conn_id=databricks_conn_id,
            source="WORKSPACE",
        )

    outlet_activity = DummyOperator(
        task_id='outlet_activity',
        dag=dag,
        outlets=[Dataset(f'Demo_Dataset')]
    )

    '''Exception Handling'''
    exception_handling = PythonOperator(
        task_id='exception_handling',
        trigger_rule='one_failed',
        python_callable=raise_pagerduty_inc,
        op_kwargs={"dag_id": dag_id, "Impact": f"No impact"},
        dag=dag
    )

'''DAG Dependencies''' 
job_Cluster_id_1 >> outlet_activity
job_Cluster_id_1 >> exception_handling
