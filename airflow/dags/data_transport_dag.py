''' Sellometer housekeeping Airflow DAG that processes minute-level datatable
into hourly level datatable, compress raw csv log files, and process backlogs
'''
import os
import imp
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
# from airflow.operators.sensors import S3KeySensor
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor


# load self defined modules
UTIL = imp.load_source('UTIL', '/home/ubuntu/eCommerce/data-processing/utility.py')

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
    ''' spark-submit pyspark script for compressing csv log files '''
    os.system(f'spark-submit --conf spark.cores.max=6 --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/log_compression.py')

def run_data_transport():
    ''' spark-submit pyspark script for minute-level to hourly table data transport '''
    os.system(f'spark-submit --conf spark.cores.max=6 --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/data_transport.py')

def run_backlog_processing():
    ''' spark-submit pyspark script for backlog processing '''
    os.system(f'spark-submit --conf spark.cores.max=6 --executor-memory=3G ' +\
    '$sparkf ~/eCommerce/data-processing/backlog_processing.py')

data_transport = PythonOperator(
    task_id='min_to_hour',
    python_callable=run_data_transport,
    dag=dag)

logs_compression = PythonOperator(
    task_id='logs_compression',
    python_callable=run_logs_compression,
    dag=dag)

process_backlogs = PythonOperator(
    task_id='process_backlogs',
    python_callable=run_backlog_processing,
    dag=dag)

check_backlog = BranchPythonOperator(
    task_id='check_backlog',
    python_callable=UTIL.check_backlog,
    dag=dag)

check_backlog >> logs_compression >> data_transport
check_backlog >> process_backlogs >> data_transport
