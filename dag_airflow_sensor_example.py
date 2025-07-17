# Imports
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# DAG Definition
@dag(
    dag_id="sensor_demo_downstream_dag",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["sensor", "external_task", "example"],
    default_args={
        "owner": "data_team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)
def downstream_dag():

    start = EmptyOperator(task_id="start")

    # Sensor to wait for external task in another DAG
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream_task",
        external_dag_id="upstream_dag",         # DAG ID to watch
        external_task_id="extract_data",        # Task ID in the upstream DAG
        mode="reschedule",                      # recommended for efficiency
        timeout=3600,                           # 1 hour max wait
        poke_interval=60,                       # check every 60 seconds
        allowed_states=["success"],             # wait until task is successful
        failed_states=["failed", "skipped"],    # treat these as failures
    )

    @task()
    def load_data():
        print("Loading data now that upstream task is complete!")

    end = EmptyOperator(task_id="end")

    start >> wait_for_upstream >> load_data() >> end

# Register DAG
downstream_dag()
