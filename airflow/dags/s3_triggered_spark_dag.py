from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import subprocess

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'
schedule = timedelta(seconds=60)
bash_cmd_template = """

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

def read_file_size(**kwargs):
    p = subprocess.Popen([f's3cmd du $s3/{src_dir}'], stdout=subprocess.PIPE, stderr=subprocess.IGNORE)
    text = p.stdout.read()
    tot_size = float(text.split(" ")[0) / 1024 / 1024 # total file size in Mbytes
    max_cores = 12 if totsize > 10 else 6
    kwargs['ti'].xcom_push(key='max_cores', value = max_cores)

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
  bash_command=f'spark-submit --conf spark.cores.max={ti.xcom_pull("max_cores")} $sparkf ~/eCommerce/data-processing/spark_aggregate.py',
  dag = dag)

move_processed_csv =  BashOperator(
  task_id='move_processed_csv',
  bash_command=f's3cmd mv s3://{bucket}/{src_dir}* s3://{bucket}/{dst_dir}',
  dag = dag)

print_new_csv_files = BashOperator(
  task_id='print_new_csv_files',
  bash_command=f's3cmd ls s3://{bucket}/{src_dir}',
  dag = dag)


file_sensor >>  print_new_csv_files >> spark_live_process  >> move_processed_csv
