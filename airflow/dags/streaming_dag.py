''' Sellometer main Airflow DAG for real-time processing and maintains daily window
'''
import os
import subprocess
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor

args = {
    'owner': 'airflow',
    'retries': 0,
    'start_date': days_ago(1),
    'depends_on_past': False,
    'wait_for_downstream':False,
    'retry_delay': timedelta(seconds=5),
    }

dag = DAG(
    dag_id='streaming',
    schedule_interval=timedelta(seconds=60),
    max_active_runs=1,
    default_args=args
    )

def run_ingestion():
    ''' spark-submit real-time processing pyspark script.
        Dynamical resource allocation by using file size information on
        AWS S3 bucket to determine resource for spark job.
    '''
    response = subprocess.check_output(f's3cmd du $s3/serverpool/', shell=True).decode('ascii')
    file_size = float(response.split(" ")[0]) / 1024 / 1024 # total file size in Mbytes
    # use extra processors when file size greater than 10 Mb
    max_cores = 12 if file_size > 20 else 8
    os.system(f'spark-submit --conf spark.cores.max={max_cores} --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/ingestion.py')

def run_time_window():
    ''' spark-submit pyspark script that maintains 24-hour window for the
     minute-level datatable on PostgreSQL DB
    '''
    os.system(f'spark-submit --conf spark.cores.max=14 --executor-memory=5G ' +\
    '$sparkf ~/eCommerce/data-processing/table_time_window.py')

new_file_sensor = S3KeySensor(
    task_id='new_csv_sensor',
    poke_interval=5, # (seconds); checking file every 5 seconds
    timeout=30, # timeout in 1 hours
    bucket_key=f"s3://maxwell-insight/serverpool/*.csv",
    bucket_name=None,
    wildcard_match=True,
    dag=dag)

spark_ingestion = PythonOperator(
    task_id='spark_ingestion',
    python_callable=run_ingestion,
    trigger_rule='none_failed',
    dag=dag)

table_time_window = PythonOperator(
    task_id='table_time_window',
    python_callable=run_time_window,
    dag=dag)

new_file_sensor >> spark_ingestion >> table_time_window
