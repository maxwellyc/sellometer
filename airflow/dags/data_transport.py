from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
import os, subprocess, sys, imp
util = imp.load_source('util', '/home/ubuntu/eCommerce/data-processing/check_backlog.py')
# sys.path.append('/home/ubuntu/eCommerce/data_processing')
# sys.path.insert(0,os.path.abspath(os.path.dirname('/home/ubuntu/eCommerce/data-processing/')))
#sys.path.insert(0,os.path.abspath(os.path.dirname('/home/ubuntu/eCommerce/data_processing')))

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'

dimensions = ['product_id', 'brand', 'category_l3'] #  'category_l1','category_l2'
events = ['purchase', 'view'] # test purchase then test view

args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'depends_on_past': True,
    'wait_for_downstream':True,
    'retry_delay': timedelta(seconds=5),
    }
dag = DAG(
    dag_id='main_spark_process',
    schedule_interval=timedelta(seconds=120),
    max_active_runs=2,
    default_args=args
    )

def run_logs_compression():
    os.system(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/log_compression.py')

def run_min_to_hour():
    os.system(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/min_to_hour.py')

min_to_hour = PythonOperator(
  task_id='min_to_hour',
  python_callable=run_min_to_hour,
  dag = dag)

logs_compression = BranchPythonOperator(
    task_id='logs_compression',
    python_callable=run_logs_compression,
    dag=dag
)

min_to_hour >> logs_compression
