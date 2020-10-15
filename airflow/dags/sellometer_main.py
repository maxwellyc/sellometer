from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
import os, subprocess, sys
# sys.path.append('/home/ubuntu/eCommerce/data_processing')
sys.path.insert(0,os.path.abspath(os.path.dirname('/home/ubuntu/eCommerce/data-processing/')))
#sys.path.insert(0,os.path.abspath(os.path.dirname('/home/ubuntu/eCommerce/data_processing')))
import check_backlog


bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'

dimensions = ['product_id', 'brand', 'category_l3'] #  'category_l1','category_l2'
events = ['purchase', 'view'] # test purchase then test view

args_1 = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'depends_on_past': True,
    'wait_for_downstream':True,
    'retry_delay': timedelta(seconds=5),
    }

args_2 = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'depends_on_past': True,
    'wait_for_downstream':True,
    'retry_delay': timedelta(minutes=1),
    }

dag_1 = DAG(
    dag_id='main_spark_process',
    schedule_interval=timedelta(seconds=120),
    max_active_runs=1,
    default_args=args_1
    )

dag_2 = DAG(
    dag_id='data_transport',
    schedule_interval=timedelta(minutes=30),
    max_active_runs=1,
    default_args=args_2
    )

def run_streaming():
    response = subprocess.check_output(f's3cmd du $s3/{src_dir}', shell=True).decode('ascii')
    file_size = float(response.split(" ")[0]) / 1024 / 1024 # total file size in Mbytes
    # use extra processors when file size greater than 10 Mb
    max_cores = 12 if file_size > 10 else 10
    print(max_cores,'spark cores executing')
    print(sys.path)
    subprocess.call(f'spark-submit --conf spark.cores.max={max_cores} ' +\
    '$sparkf streaming.py')

def run_logs_compression():
    os.chdir("~/eCommerce/data-processing/")
    subprocess.call(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/log_compression.py')

def run_min_to_hour():
    os.chdir("~/eCommerce/data-processing/")
    subprocess.call(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/min_to_hour.py')

def run_backlog_processing():
    os.chdir("~/eCommerce/data-processing/")
    subprocess.call(f'spark-submit --conf spark.cores.max=4 ' +\
    '$sparkf ~/eCommerce/data-processing/backlog_processing.py')

new_file_sensor = S3KeySensor(
    task_id='new_csv_sensor',
    poke_interval= 3, # (seconds); checking file every 4 seconds
    timeout=60 * 60, # timeout in 1 hours
    bucket_key=f"s3://{bucket}/{src_dir}*.csv",
    bucket_name=None,
    wildcard_match=True,
    dag=dag_1)

spark_live_process = PythonOperator(
  task_id='spark_live_process',
  python_callable=run_streaming,
  trigger_rule='none_failed',
  dag = dag_1
  )

check_backlog = BranchPythonOperator(
    task_id='check_backlog',
    python_callable=check_backlog.collect_backlogs,
    dag = dag_1)

process_backlogs = PythonOperator(
    task_id='process_backlogs',
    python_callable=run_backlog_processing,
    dag = dag_1)

min_to_hour = PythonOperator(
  task_id='min_to_hour',
  python_callable=run_min_to_hour,
  dag = dag_2)

logs_compression = BranchPythonOperator(
    task_id='logs_compression',
    python_callable=run_logs_compression,
    dag=dag_2
)

dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag_1
)
# sensor = ExternalTaskSensor(
#     task_id = 'sensor',
#     external_dag_id = 'main_spark_process',
#     external_task_id = 'spark_live_process'
# )


new_file_sensor >> check_backlog >>  dummy_task >> spark_live_process
check_backlog >> process_backlogs >> spark_live_process

min_to_hour >> logs_compression
