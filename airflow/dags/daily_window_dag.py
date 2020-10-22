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
    'start_date': days_ago(1),
    'depends_on_past': False,
    'wait_for_downstream':False,
    'retry_delay': timedelta(seconds=5),
    }

dag = DAG(
    dag_id='daily_window',
    schedule_interval=timedelta(hours=1),
    max_active_runs=1,
    default_args=args
    )

# Use 14 cores to block other process from working on datatable to stop race condition
def run_daily_window():
    os.system(f'spark-submit --conf spark.cores.max=14 --executor-memory=5G ' +\
    '$sparkf ~/eCommerce/data-processing/daily_window.py')

daily_window = PythonOperator(
    task_id='daily_window',
    python_callable=run_daily_window,
    dag=dag)
