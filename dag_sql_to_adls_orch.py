# Imports
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from io import StringIO

# Dag Definition
@dag(
    dag_id="direct_sql_to_adls_upload",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["sqlserver", "adls", "azure", "no_adf"],
    default_args={
        "owner": "data_team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def direct_sql_to_adls():

    start = EmptyOperator(task_id="start")

    @task()
    def extract_from_sql():
        """Query data from SQL Server and return as DataFrame."""
        hook = MsSqlHook(mssql_conn_id="mssql_default")
        query = "SELECT TOP 100 * FROM Sales"  # Customize your table/query
        df = hook.get_pandas_df(sql=query)
        return df.to_json(orient="records")  # serialized to pass between tasks

    @task()
    def upload_to_adls(json_data: str):
        """Upload data to ADLS Gen2 as CSV."""
        df = pd.read_json(json_data)

        account_name = Variable.get("adls_account_name")
        container_name = Variable.get("adls_container_name")  # same as file system
        sas_token = Variable.get("adls_sas_token")
        target_path = Variable.get("target_path", default_var="exports/sales.csv")

        # Create ADLS DataLakeServiceClient
        datalake_service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=sas_token
        )

        file_system_client = datalake_service_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.get_file_client(file_path=target_path)

        # Convert DataFrame to CSV buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        # Upload CSV to ADLS Gen2
        file_client.create_file()
        file_client.append_data(data=csv_data, offset=0, length=len(csv_data))
        file_client.flush_data(len(csv_data))

        print(f"Uploaded to ADLS Gen2: {container_name}/{target_path}")

    end = EmptyOperator(task_id="end")

    # DAG flow Dependencies
    start >> extract_from_sql() >> upload_to_adls() >> end

# DAG registration
direct_sql_to_adls()
