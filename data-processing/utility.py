''' Utility functions that deals with timestamps, file handling with AWS S3
and IO operation on postgreSQL database.
'''
import datetime as dt
import os
from boto3 import client
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext

def spark_init(s_name):
    ''' Create spark session and SQL context
    '''
    conf = SparkConf().setAppName(s_name)
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sql_c = SQLContext(sc)
    return sql_c, spark

# Date, time, file name related ================================================

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    ''' Convert string to datetime object
    '''
    return dt.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    ''' Convert datetime object to string
    '''
    return dt_obj.strftime(time_format)

def select_time_window(df, start_tick, end_tick):
    ''' Filter dataframe to only keep entries between timestamps
        start_tick (inclusive) and end_tick (exclusive)
    '''
    df1 = df.filter((df.event_time < end_tick) & (df.event_time >= start_tick))
    return df1

def remove_server_num(f_name, suffix='.csv', server_num=True):
    ''' Remove server # from file name in order to get time
        ie. '2019-10-01-01-00-00-3.csv' -> '2019-10-01-01-00-00'
    '''
    if "temp/" in f_name:
        f_name = f_name[5:]
    if server_num:
        return '-'.join(f_name.split(suffix)[0].split('-')[:-1])
    return f_name.split(suffix)[0]

def get_latest_time_from_db(spark, evt='view', dim='category_l3',
                            suffix='minute', max_time=True):
    ''' From postgreSQL database, collect min or max of event_time
        default to evt='view' because this event type is much more populated
        default to dim='category_l3' because this dimension is more collapsed,
        resulting in a smaller table to be read but still maintains all possible
        event_time.
        Returns default time if the t1 tables are not created yet.
        Returns a datetime.datetime object.
    '''
    sort_order = "DESC" if max_time else "ASC"

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
        .option("user", os.environ['psql_username'])\
        .option("password", os.environ['psql_pw'])\
        .option("driver", "org.postgresql.Driver")\
        .load()

        return df.select('event_time').collect()[0][0]

    except:
        return str_to_datetime('2019-10-01-00-00-00')

def folder_time_range(lof, time_format='%Y-%m-%d-%H-%M-%S',
                      suffix=".csv", server_num=True):
    ''' Returns range of time indicated by files in lof (list of files)
        Returns 2 datetime.datetime objects
    '''
    file_times = []
    for f_name in lof:
        try:
            t = remove_server_num(f_name, suffix, server_num)
            t = str_to_datetime(t, time_format)
            file_times.append(t)
        except:
            continue
    if file_times:
        return min(file_times), max(file_times)
    return [str_to_datetime("2019-09-30-23-00-00")]*2

# AWS S3 related ============================================================

def move_s3_file(bucket, src_dir, dst_dir, f_name='*.csv'):
    ''' Move files (file name = <f_name>) on AWS S3 from <src_dir> to <dst_dir>
    '''
    os.system(f's3cmd mv s3://{bucket}/{src_dir}{f_name} s3://{bucket}/{dst_dir}')

def remove_s3_file(bucket, src_dir, prefix):
    ''' Remove files in AWS S3 with specific {prefix}
        src_dir should end with '/'
    '''
    os.system(f's3cmd rm s3://{bucket}/{src_dir}{prefix}*.csv')

def list_s3_files(src_dir="serverpool", bucket='maxwell-insight'):
    ''' List all files in a AWS S3 directory, return as list
    '''
    src_dir += "/"
    conn = client('s3')
    lof = [key['Key'].replace(src_dir, "", 1)
           for key in conn.list_objects(Bucket=bucket, Prefix=src_dir)['Contents']]
    return lof

def check_backlog():
    ''' Check if backlog folder is empty. If not empty, this function
        will return the Airflow task name for backlog processing to trigger
        backlog processing, otherwise this will trigger the compression process
        of log files.
    '''
    try:
        lof = list_s3_files(dir='backlogs')
        for f in lof:
            if ".csv" in f:
                return 'process_backlogs'
    except:
        pass
    return 'logs_compression'


# Read / Write data related ====================================================

def read_s3_to_df(sql_c, bucket='maxwell-insight',
                  src_dir='serverpool/', prefix=''):
    ''' Spark read data from AWS S3 into spark dataframe
    '''
    s3file = f's3a://{bucket}/{src_dir}{prefix}*.csv'
    try:
        return sql_c.read.csv(s3file, header=True)
    except:
        return None

def read_sql_to_df(spark, t0='2019-10-01 00:00:00', t1='2119-10-01 00:00:00',
                   event='purchase', dim='product_id', suffix='minute'):
    ''' Read postgreSQL datatable into spark dataframe BETWEEN t0 AND t1
        using SQL query
    '''
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
    .option("user", os.environ['psql_username'])\
    .option("password", os.environ['psql_pw'])\
    .option("driver", "org.postgresql.Driver")\
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
    .option("user", os.environ['psql_username'])\
    .option("password", os.environ['psql_pw'])\
    .option("driver", "org.postgresql.Driver")\
    .mode(mode)\
    .save()
    return
