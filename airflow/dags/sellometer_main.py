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
    'depends_on_past': False,
    'wait_for_downstream':True,
    'retry_delay': timedelta(seconds=5),
    }

dag = DAG(
    dag_id='main_spark_process',
    schedule_interval=timedelta(seconds=120),
    max_active_runs=1,
    default_args=args
    )

def run_streaming():
    response = subprocess.check_output(f's3cmd du $s3/{src_dir}', shell=True).decode('ascii')
    file_size = float(response.split(" ")[0]) / 1024 / 1024 # total file size in Mbytes
    # use extra processors when file size greater than 10 Mb
    max_cores = 12 if file_size > 10 else 10
    print(max_cores,'spark cores executing')
    os.system(f'spark-submit --conf spark.cores.max={max_cores} ' +\
    '$sparkf ~/eCommerce/data-processing/streaming.py')

def run_backlog_processing():
    os.system(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/backlog_processing.py')

new_file_sensor = S3KeySensor(
    task_id='new_csv_sensor',
    poke_interval= 3, # (seconds); checking file every 4 seconds
    timeout=60 * 60, # timeout in 1 hours
    bucket_key=f"s3://{bucket}/{src_dir}*.csv",
    bucket_name=None,
    wildcard_match=True,
    dag=dag)

spark_live_process = PythonOperator(
  task_id='spark_live_process',
  python_callable=run_streaming,
  trigger_rule='none_failed',
  dag = dag
  )

check_backlog = BranchPythonOperator(
    task_id='check_backlog',
    python_callable=util.collect_backlogs,
    dag = dag)

process_backlogs = PythonOperator(
    task_id='process_backlogs',
    python_callable=run_backlog_processing,
    dag = dag)

dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag
)

def run_min_to_hour():
    os.system(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/min_to_hour.py')

min_to_hour = PythonOperator(
  task_id='min_to_hour',
  python_callable=run_min_to_hour,
  dag = dag)


new_file_sensor >> check_backlog >>  dummy_task >> spark_live_process >> min_to_hour
check_backlog >> process_backlogs >> spark_live_process >> min_to_hour
