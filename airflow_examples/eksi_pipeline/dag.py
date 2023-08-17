from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# default parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
}

# python with exec()
def execute_eksi():
    exec(open('./eksi.py').read())

# Creating the DAG
with DAG('eksi_and_cleaning_dag',
         default_args=default_args,
         schedule_interval="0 */3 * * *",  # runs every three hours at **:00
         catchup=False) as dag:

    eksi_task = PythonOperator(
        task_id='eksi_task',
        python_callable=execute_eksi,
    )

    cleaning_task = BashOperator(
        task_id='cleaning_task',
        bash_command='spark-submit ./cleaning_pyspark.py',
    )

    # order of execution
    eksi_task >> cleaning_task