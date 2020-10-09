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

def find_max_cores():
    response = subprocess.check_output(f's3cmd du $s3/{src_dir}', shell=True).decode('ascii')
    file_size = float(response.split(" ")[0]) / 1024 / 1024 # total file size in Mbytes
    max_cores = 12 if file_size > 10 else 6
    os.environ['max_cores'] = str(max_cores)
    print ("max_cores = ", max_cores)

file_sensor = S3KeySensor(
    task_id='new_csv_sensor',
    poke_interval= 1, # (seconds); checking file every half an hour
    timeout=60 * 60, # timeout in 1 hours
    bucket_key=f"s3://{bucket}/{src_dir}*.csv",
    bucket_name=None,
    wildcard_match=True,
    dag=dag)

spark_live_process = BashOperator(
  task_id='spark_live_process',
  bash_command='spark-submit --conf spark.cores.max=$max_cores' +\
  '$sparkf ~/eCommerce/data-processing/spark_aggregate.py',
  dag = dag)

find_max_cores = PythonOperator(
  task_id='find_max_cores',
  python_callable=find_max_cores,
  dag=dag
)

move_processed_csv =  BashOperator(
  task_id='move_processed_csv',
  bash_command=f's3cmd mv s3://{bucket}/{src_dir}* s3://{bucket}/{dst_dir}',
  dag = dag)

# print_new_csv_files = BashOperator(
#   task_id='print_new_csv_files',
#   bash_command=f's3cmd ls s3://{bucket}/{src_dir}',
#   dag = dag)


file_sensor >>  find_max_cores >> spark_live_process  >> move_processed_csv
