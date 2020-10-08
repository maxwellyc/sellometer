from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'
schedule = timedelta(seconds=60)

args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=5),
    }

dag = DAG(
    dag_id='s3_key_trigger',
    schedule_interval=schedule,
    default_args=args
    )

def move_s3_file_after_spark_process(bucket, src_dir, dst_dir):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)

    for o in my_bucket.objects.filter(Prefix=src_dir):
        f_name = o.key.split(src_dir)[-1]
        if not f_name: continue
        print(f_name)
        s3.Object(bucket, dst_dir + f_name ).copy_from(CopySource= bucket + "/" + o.key)
        s3.Object(bucket, o.key).delete()

file_sensor = S3KeySensor(
    task_id='new_csv_sensor',
    poke_interval= 1, # (seconds); checking file every half an hour
    timeout=60 * 60, # timeout in 1 hours
    bucket_key=f"s3://{bucket}/{src_dir}/",
    bucket_name=None,
    wildcard_match=True,
    dag=dag)

spark_live_process = BashOperator(
  task_id='spark_live_process',
  bash_command='spark-submit $sparkf ~/eCommerce/data-processing/spark_aggregate.py',
  dag = dag)

move_processed_csv = PythonOperator(task_id='move_processed_csv',
    provide_context=True,
    python_callable=move_s3_file_after_spark_process,
    dag=dag)

file_sensor >> spark_live_process >>  move_processed_csv
