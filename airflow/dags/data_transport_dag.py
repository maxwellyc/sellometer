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
    dag_id='data_transport',
    schedule_interval=timedelta(minutes=15),
    max_active_runs=1,
    default_args=args
    )


def run_logs_compression():
    os.system(f'spark-submit --conf spark.cores.max=6 --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/log_compression.py')

def run_data_transport():
    os.system(f'spark-submit --conf spark.cores.max=6 --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/data_transport.py')

data_transport = PythonOperator(
  task_id='min_to_hour',
  python_callable=run_data_transport,
  dag = dag)

logs_compression = PythonOperator(
    task_id='logs_compression',
    python_callable=run_logs_compression,
    dag=dag)

dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag)

process_backlogs = PythonOperator(
    task_id='process_backlogs',
    python_callable=run_backlog_processing,
    dag = dag)

check_backlog = BranchPythonOperator(
    task_id='check_backlog',
    python_callable=util.collect_backlogs,
    trigger_rule='one_success',
    dag = dag)

check_backlog >> logs_compression >> data_transport
check_backlog >> process_backlogs >> data_transport
