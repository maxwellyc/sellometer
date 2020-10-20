from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
import datetime, os
from boto3 import client

def spark_init(s_name):
    ''' Create spark session and SQL context '''
    conf = SparkConf().setAppName()
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sql_c = SQLContext(sc)
    return sql_c, spark

# Date, time related utility functions =======================================

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    ''' Convert string to datetime object '''
    return datetime.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    ''' Convert datetime object to string '''
    return dt_obj.strftime(time_format)

def remove_server_num(f_name):
    ''' Remove server # from file name in order to get time
        ie. '2019-10-01-01-00-00-3.csv' -> '2019-10-01-01-00-00'
    '''
    return '-'.join(f_name.strip(".csv").split('-')[:-1])

def get_latest_time_from_db(spark,evt='view',dim='brand',suffix='minute',max_time=True):
    ''' From postgreSQL database, collect min or max of event_time
        default to evt='view' because this event type is much more populated
        default to dim='brand' because this dimension is more collapsed, resulting
        in a smaller table but without lossing all possible event_time
        Return default time if the t1 tables are not created yet.
    '''
    sort_order = "DESC" if latest else "ASC"

    try:
        query = f"""
        (SELECT event_time FROM {evt}_{dim}_{suffix}
        ORDER BY event_time {sort_order}
        LIMIT 1
        ) as foo
        """

        df = spark.read \
            .format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce") \
        .option("dbtable", query) \
        .option("user",os.environ['psql_username'])\
        .option("password",os.environ['psql_pw'])\
        .option("driver","org.postgresql.Driver")\
        .load()

        return df.select('event_time').collect()[0][0]

    except Exception as e:
        return util.str_to_datetime('2019-10-01-00-00-00')

# AWS S3 IO related ============================================================

def move_s3_file(bucket, src_dir, dst_dir, f_name='*.csv'):
    ''' Move files (file name = <f_name>) on AWS S3 from <src_dir> to <dst_dir>'''
    os.system(f's3cmd mv s3://{bucket}/{src_dir}{f_name} s3://{bucket}/{dst_dir}')

def list_s3_files(dir="serverpool", bucket = 'maxwell-insight'):
    ''' List files in AWS S3 directory and return as list'''
    dir += "/"
    conn = client('s3')
    list_of_files = [key['Key'].replace(dir,"",1)
    for key in conn.list_objects(Bucket=bucket, Prefix=dir)['Contents']]
    return list_of_files

def peek_backlogs():
    ''' Check if backlog folder is empty. If not empty, this function
        will return the Airflow task name for backlog processing to trigger
        backlog processing, otherwise this will trigger the compression process
        of log files.
    '''
    try:
        lof = list_s3_files(dir = 'backlogs')
        for f in lof:
            if ".csv" in f:
                return 'process_backlogs'
    except Exception as e:
        continue
    return 'dummy_task'

# Read / Write data related ====================================================

def read_s3_to_df(sql_c, spark, bucket='maxwell-insight', src_dir='serverpool/'):
    ''' Spark read data from AWS S3 into spark dataframe'''
    s3file = f's3a://{bucket}/{src_dir}*.csv'
    try:
        df = sql_c.read.csv(s3file, header=True)
    except:
        return
    return df

def read_sql_to_df(spark, t0='2019-10-01 00:00:00', t1='2119-10-01 00:00:00',
                    event='purchase', dim='product_id',suffix='minute'):
    query = f"""
    (SELECT * FROM {event}_{dim}_{suffix}
    WHERE event_time BETWEEN \'{t0}\' and \'{t1}\'
    ORDER BY event_time
    ) as foo
    """
    df = spark.read \
        .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce") \
    .option("dbtable", query) \
    .option("user",os.environ['psql_username'])\
    .option("password",os.environ['psql_pw'])\
    .option("driver","org.postgresql.Driver")\
    .load()
    return df

def write_to_psql(df, event, dim, mode, suffix):
    ''' Write Spark dataframe to postgreSQL table
        Suffix can be 'minute', 'hour', 'rank',
        The former two corresponds to two tiers of datatable,
        The rank table is for ranking
    '''
    df.write\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce")\
    .option("dbtable", f"{event}_{dim}_{suffix}")\
    .option("user",os.environ['psql_username'])\
    .option("password",os.environ['psql_pw'])\
    .option("driver","org.postgresql.Driver")\
    .mode(mode)\
    .save()
    return
