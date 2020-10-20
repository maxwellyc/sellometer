from datetime import timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
import os, subprocess, sys, imp

bucket = 'maxwell-insight'
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
    dag_id='main_spark_process',
    schedule_interval=timedelta(seconds=60),
    max_active_runs=1,
    default_args=args
    )

def run_ingestion():
    response = subprocess.check_output(f's3cmd du $s3/serverpool/', shell=True).decode('ascii')
    file_size = float(response.split(" ")[0]) / 1024 / 1024 # total file size in Mbytes
    # use extra processors when file size greater than 10 Mb
    max_cores = 12 if file_size > 20 else 8
    print(max_cores,'spark cores executing')
    os.system(f'spark-submit --conf spark.cores.max={max_cores} --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/ingestion.py')

def run_backlog_processing():
    os.system(f'spark-submit --conf spark.cores.max=12 --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/backlog_processing.py')


new_file_sensor = S3KeySensor(
    task_id='new_csv_sensor',
    poke_interval= 5, # (seconds); checking file every 5 seconds
    timeout= 30, # timeout in 1 hours
    bucket_key=f"s3://{bucket}/serverpool/*.csv",
    bucket_name=None,
    wildcard_match=True,
    dag=dag)

spark_ingestion = PythonOperator(
  task_id='spark_live_process',
  python_callable=run_ingestion,
  trigger_rule='none_failed',
  dag = dag)

new_file_sensor >> spark_ingestion
