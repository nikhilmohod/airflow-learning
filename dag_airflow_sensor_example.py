# Imports
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# DAG Definition
@dag(
    dag_id="sensor_demo_with_file_sensor",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["sensor", "external_task", "file_sensor", "example"],
    default_args={
        "owner": "data_team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email": ["data_team@example.com"],  # Email to notify on failures
        "email_on_failure": True,
        "email_on_retry": False,
    }
)
def downstream_dag():

    start = EmptyOperator(task_id="start")

    # Sensor to wait for external task in another DAG
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream_task",
        external_dag_id="upstream_dag",          # DAG ID to watch
        external_task_id="extract_data",         # Task ID in the upstream DAG
        mode="reschedule",                       # recommended for efficiency
        timeout=3600,                            # 1 hour max wait
        poke_interval=60,                        # check every 60 seconds
        allowed_states=["success"],              # wait until task is successful
        failed_states=["failed", "skipped"],     # treat these as failures
    )

    # Sensor to wait for a file to appear on the filesystem
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/data_ready.flag",        # Full path to the file to wait for
        poke_interval=30,                       # check every 30 seconds
        timeout=1800,                           # max wait 30 minutes
        mode="reschedule",                      # efficient mode to free worker slots
    )

    @task()
    def load_data():
        print("Loading data now that upstream task is complete and file is present!")

    end = EmptyOperator(task_id="end")

    # Email alert on success
    success_email = EmailOperator(
        task_id='send_success_email',
        to='data_team@example.com',
        subject='DAG Success: sensor_demo_with_file_sensor',
        html_content='The DAG has completed successfully!',
        trigger_rule='all_success'
    )

    # Email alert on failure
    failure_email = EmailOperator(
        task_id='send_failure_email',
        to='data_team@example.com',
        subject='DAG Failure: sensor_demo_with_file_sensor',
        html_content='The DAG has failed. Please investigate.',
        trigger_rule='one_failed'
    )

    # Define task dependencies
    start >> wait_for_upstream >> wait_for_file >> load_data() >> end
    end >> success_email
    [wait_for_upstream, wait_for_file, load_data] >> failure_email

# Register DAG
downstream_dag()
