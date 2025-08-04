from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

def _check():
    print("file written")

dag = DAG(dag_id='test_dag', start_date=datetime(2022, 8, 7), schedule_interval="@daily", catchup=False)

task_1 = PythonOperator(
    task_id = 'task_1',
    python_callable = _check,
    dag=dag
)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='echo "test"',
    dag=dag
)