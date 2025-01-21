from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define common options for Dataflow jobs
dataflow_options = {
    'project': 'sai-project-2024-441608',
    'region': 'us-central1',
    'temp_location': 'gs://temp_buckdet/temp',
    'staging_location': 'gs://temp_buckdet/staging',
    'runner': 'DataflowRunner',
    'input': 'gs://temp_buckdet/',
    'output': 'gs://raw_buckett/',
}

# DAG settings
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG with a daily schedule
with DAG(
    dag_id='daily_file_ingestion',
    default_args=default_args,
    description='Daily ingestion of data from one bucket to another bucket using Dataflow',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
) as dag:

    # Define the Dataflow jobs
    task1 = DataflowCreatePythonJobOperator(
        task_id='submit_raw_layer_job',
        py_file='gs://temp_buckdet/bronze_layer1.py',  # Path to your Dataflow Python file in GCS
        options=dataflow_options,
        gcp_conn_id='my_connection',  # Airflow connection ID for Google Cloud Platform
        dag=dag,
    )

    task2 = DataflowCreatePythonJobOperator(
        task_id='submit_silver_layer_job',
        py_file='gs://temp_buckdet/bronze_layer.py',  # Path to your Dataflow Python file in GCS
        options=dataflow_options,
        gcp_conn_id='my_connection',  # Airflow connection ID for Google Cloud Platform
        dag=dag,
    )

    task3 = DataflowCreatePythonJobOperator(
        task_id='submit_third_job',
        py_file='gs://temp_buckdet/bronze_layer1.py',  # Path to your Dataflow Python file in GCS
        options=dataflow_options,
        gcp_conn_id='my_connection',  # Airflow connection ID for Google Cloud Platform
        dag=dag,
    )

    # Set up dependencies
    task1>>(task2,task3)