from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.baseoperator import chain, cross_downstream
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from airflow.models import Variable



default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
# ,
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'email': 'd4smart@gmail.com'

def _download_data(ti, my_param, **kwargs):
    file_path='/tmp/data.txt'
    with open(file_path, 'w') as f:
        f.write(my_param)
    print("file written")
    print("kwargs:")
    print(kwargs) # Prints the context
    print(kwargs['ds']) # Prints the execution date from context. Another option is the put "ds" as one of function param.
    ti.xcom_push(key='my_key', value=27)
    return file_path

def _checking_data(ti):
    my_xcom = ti.xcom_pull(key='return_value', task_ids=['download_data'])
    my_xcom_key = ti.xcom_pull(key='my_key', task_ids=['download_data'])
    print('checking data at path from xcom:')
    print(my_xcom)
    print('Key value:')
    print(my_xcom_key)

def _failure(context):
    print('On failure callback new')
    print(context)

def _variable_extract():
    secret_variable = Variable.get("sample_dag_partner_secret")
    print(secret_variable)
    partner_settings_variable = Variable.get("sample_dag_partner", deserialize_json = True)
    print(partner_settings_variable)
    print(partner_settings_variable['name'])

def _variable_pass(partner_name):
    print(partner_name)

def _env_variable_extract():
    env_variable = Variable.get("NEW_ENV_VAR")
    print(env_variable)

with DAG(dag_id='sample_dag', default_args=default_args, schedule_interval="@daily", 
start_date=datetime(2021, 1 ,1), catchup=False) as dag:
# start_date=days_ago(3)
##, max_active_runs=20

    download_data = PythonOperator(
        task_id = 'download_data',
        python_callable = _download_data,
        op_kwargs={'my_param': 'My first Py Airflow task :)'}
    )

    checking_data = PythonOperator(
        task_id = 'checking_data',
        python_callable=_checking_data
    )

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='fs_default',
        filepath='data.txt'
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='echo "test"',
        on_failure_callback=_failure
    )

    variable_extract = PythonOperator(
        task_id='variable_extract',
        python_callable = _variable_extract
    )

    variable_pass = PythonOperator(
        task_id='variable_pass',
        python_callable=_variable_pass,
        # This will create con. to metadata db everytime when the dag parses
        #op_args=[Variable.get("sample_dag_partner", deserialize_json = True)['name']] 
        # instead do as below using template engine
        op_args=["{{ var.json.sample_dag_partner.name }}"] 
    )

    env_variable_extract = PythonOperator(
        task_id='env_variable_extract',
        python_callable = _env_variable_extract
    )

    # chain(download_data, checking_data, wait_for_file, processing_data)

    # task_1 = DummyOperator(
    #     task_id='task_1'
    # )