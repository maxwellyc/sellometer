from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import os, subprocess

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'
schedule = timedelta(seconds=60)

args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'depends_on_past': True,
    'wait_for_downstream':True,
    'retry_delay': timedelta(seconds=5),
    }

dag = DAG(
    dag_id='s3_key_trigger',
    schedule_interval=schedule,
    max_active_runs=1,
    default_args=args
    )

def spark_live_process():
    response = subprocess.check_output(f's3cmd du $s3/{src_dir}', shell=True).decode('ascii')
    file_size = float(response.split(" ")[0]) / 1024 / 1024 # total file size in Mbytes
    # use extra processors when file size greater than 9 Mb
    max_cores = 12 if file_size > 9 else 6
    print(max_cores,'spark cores executing')
    os.system(f'spark-submit --conf spark.cores.max={max_cores} ' +\
    '$sparkf ~/eCommerce/data-processing/stream_to_minute.py')

file_sensor = S3KeySensor(
    task_id='new_csv_sensor',
    poke_interval= 1, # (seconds); checking file every half an hour
    timeout=60 * 60, # timeout in 1 hours
    bucket_key=f"s3://{bucket}/{src_dir}*.csv",
    bucket_name=None,
    wildcard_match=True,
    dag=dag)

spark_live_process = PythonOperator(
  task_id='spark_live_process',
  python_callable=spark_live_process,
  dag = dag)

move_processed_csv =  BashOperator(
  task_id='move_processed_csv',
  bash_command=f's3cmd mv s3://{bucket}/{src_dir}* s3://{bucket}/{dst_dir}',
  dag = dag)


file_sensor >> spark_live_process  >> move_processed_csv
