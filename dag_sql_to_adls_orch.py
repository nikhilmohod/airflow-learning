# --------------------------------------------
# Required Imports
# --------------------------------------------

from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator

from datetime import timedelta
from airflow.utils.dates import days_ago

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from io import StringIO

# --------------------------------------------
# DAG Definition
# --------------------------------------------

@dag(
    dag_id="direct_sql_to_adls_upload",
    start_date=days_ago(1),
    schedule=None,  # Manual run only
    catchup=False,
    tags=["sqlserver", "adls", "azure", "no_adf"],
    default_args={
        "owner": "data_team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def direct_sql_to_adls():

    # Dummy start task
    start = EmptyOperator(task_id="start")

    # ----------------------------------------
    # Task 1: Extract Data from SQL Server
    # ----------------------------------------
    @task()
    def extract_from_sql():
        """
        Query data from SQL Server and return it as a serialized JSON string.
        """
        # Connect to SQL Server via Airflow connection
        hook = MsSqlHook(mssql_conn_id="mssql_default")

        # Simple query (customize as needed)
        query = "SELECT TOP 100 * FROM Sales"

        # Load query result into pandas DataFrame
        df = hook.get_pandas_df(sql=query)

        # Return as JSON string (to pass between tasks)
        return df.to_json(orient="records")

    # ----------------------------------------
    # Task 2: Upload JSON data to ADLS as CSV
    # ----------------------------------------
    @task()
    def upload_to_adls(json_data: str):
        """
        Convert JSON data to CSV and upload it to Azure Data Lake Gen2.
        """
        # Convert JSON back to DataFrame
        df = pd.read_json(json_data)

        # --- Hardcoded Example Configuration for Demo Purposes ---
        account_name = "your_adls_account"
        container_name = "your-container"
        sas_token = "your_sas_token"  # Note: use with caution in real code
        target_path = "exports/sales.csv"  # Target location in ADLS

        # Initialize ADLS client using SAS token
        datalake_service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=sas_token
        )

        # Get reference to file system and file path
        file_system_client = datalake_service_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.get_file_client(file_path=target_path)

        # Convert DataFrame to CSV string in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        # Create and upload the file to ADLS Gen2
        file_client.create_file()
        file_client.append_data(data=csv_data, offset=0, length=len(csv_data))
        file_client.flush_data(len(csv_data))

        print(f"Uploaded file to ADLS: {container_name}/{target_path}")

    # Dummy end task
    end = EmptyOperator(task_id="end")

    # ----------------------------------------
    # Email Notifications
    # ----------------------------------------

    email_on_success = EmailOperator(
        task_id="send_success_email",
        to="team@example.com",
        subject="DAG direct_sql_to_adls_upload Succeeded",
        html_content="The DAG direct_sql_to_adls_upload completed successfully.",
        trigger_rule="all_success"
    )

    email_on_failure = EmailOperator(
        task_id="send_failure_email",
        to="team@example.com",
        subject="DAG direct_sql_to_adls_upload Failed",
        html_content="The DAG direct_sql_to_adls_upload has failed. Please investigate.",
        trigger_rule="one_failed"
    )

    # ----------------------------------------
    # DAG Task Flow
    # ----------------------------------------

    (
        start
        >> extract_from_sql()
        >> upload_to_adls()
        >> end
        >> email_on_success
    )

    # Failure path
    start >> email_on_failure  # Will be triggered if any previous task fails

# --------------------------------------------
# DAG Registration
# --------------------------------------------

direct_sql_to_adls()
