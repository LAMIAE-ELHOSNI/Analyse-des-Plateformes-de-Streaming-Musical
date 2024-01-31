from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'run_extract_file_dag',
    default_args=default_args,
    description='A DAG to run extract_file.py',
    schedule_interval=None,
)

# Define a BashOperator to run your Python script
run_extraction_task = BashOperator(
    task_id='run_extraction_task',
    bash_command='python /opt/airflow/dags/test.py',  # Replace with the actual path to your Python script
    dag=dag,
)

run_extraction_task
