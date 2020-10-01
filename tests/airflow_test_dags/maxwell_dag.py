# airflow related
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2019, 9, 28),
    'retry_delay': timedelta(seconds=5),
}

def print_time():
 # code that writes our data from source 1 to s3
   print ("calling print_date function!\t\n================================")
   print (datetime.utcnow())   
   return 
def source2_to_hdfs():
 # code that writes our data from source 2 to hdfs
 # kwargs: keyword arguments containing context parameters for the run.
   print ("calling source2_to_hdfs function!\t\n================================")
   return
def source3_to_s3():
 # code that writes our data from source 3 to s3
   print ("calling source3_to_s3 function!\t\n================================")
   return

dag = DAG(
  dag_id='maxwell_first_dag',
  description='Maxwell DAG test',
  schedule_interval = None,
  default_args=default_args)


prt_time = PythonOperator(
  task_id='print_time',
  python_callable=print_time,
  dag=dag)

src2_hdfs = PythonOperator(
  task_id='source2_to_hdfs',
  python_callable=source2_to_hdfs,
#  op_kwargs = {'config' : config},
#  provide_context=True,
  dag=dag
)

src3_s3 = PythonOperator(
  task_id='source3_to_s3',
  python_callable=source3_to_s3,
  dag=dag)

spark_job = BashOperator(
  task_id='spark_aggregate',
  bash_command='spark-submit $sparkf ~/eCommerce/MVP/spark_aggregate.py',
  dag = dag)

# setting dependencies
prt_time >> spark_job
src2_hdfs >> spark_job
src3_s3 >> spark_job

# for parallel jobs, use list [t1, t2], eg:
# src1_s3 >> [spark_job, src2_hdfs]
# jobs in brackets will run in parallel, I think... MCedit 09/30/20

# for Airflow <v1.7
# spark_job.set_upstream(src1_s3)
# spark_job.set_upstream(src2_hdfs)
# alternatively using set_downstream
# src3_s3.set_downstream(spark_job)
