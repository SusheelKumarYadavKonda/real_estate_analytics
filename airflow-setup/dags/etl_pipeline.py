from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.utils import process_and_transform_files, combine_files, load_data_into_redshift
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='etl_single_task_process_files',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    process_files_task = PythonOperator(
        task_id='process_and_transform_files',
        python_callable=process_and_transform_files,
        provide_context=True,
    )
    combine_task = PythonOperator(
        task_id='combine_files',
        python_callable=combine_files,
        provide_context=True,
    )

process_files_task>>combine_task