from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
import os, subprocess, sys, imp


bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'

dimensions = ['product_id', 'brand', 'category_l3'] #  'category_l1','category_l2'
events = ['purchase', 'view'] # test purchase then test view

args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'depends_on_past': False,
    'wait_for_downstream':False,
    'retry_delay': timedelta(seconds=5),
    }
dag = DAG(
    dag_id='data_transport',
    schedule_interval=timedelta(minutes=10),
    max_active_runs=1,
    default_args=args
    )

def run_logs_compression():
    os.system(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/log_compression.py')

def run_min_to_hour():
    os.system(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/min_to_hour.py')

def min_data_window()

min_to_hour = PythonOperator(
  task_id='min_to_hour',
  python_callable=run_min_to_hour,
  dag = dag)

logs_compression = PythonOperator(
    task_id='logs_compression',
    python_callable=run_logs_compression,
    dag=dag)

min_to_hour >> logs_compression
