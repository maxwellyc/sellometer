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
    'retries': 0,
    'start_date': days_ago(0,minute=60),
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

subdag = DAG(
    dag_id='data_transport',
    schedule_interval=timedelta(minutes=30),
    max_active_runs=1,
    default_args=args
    )

def run_logs_compression():
    os.system(f'spark-submit --conf spark.cores.max=6 ' +\
    '$sparkf ~/eCommerce/data-processing/log_compression.py')

def run_data_transport():
    os.system(f'spark-submit --conf spark.cores.max=6 ' +\
    '$sparkf ~/eCommerce/data-processing/data_transport.py')

def run_daily_window():
    os.system(f'spark-submit --conf spark.cores.max=6 ' +\
    '$sparkf ~/eCommerce/data-processing/daily_window.py')

data_transport = PythonOperator(
  task_id='min_to_hour',
  python_callable=run_data_transport,
  dag = dag)

logs_compression = PythonOperator(
    task_id='logs_compression',
    python_callable=run_logs_compression,
    dag=dag)

daily_window = PythonOperator(
    task_id='daily_window',
    python_callable=run_daily_window,
    dag=subdag)


data_transport >> logs_compression
