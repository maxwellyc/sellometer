from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import time, datetime, os
from boto3 import client

def spark_init():
    # initialize spark session and spark context####################################
    conf = SparkConf().setAppName("logs_compression")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sql_c = SQLContext(sc)
    return sql_c, spark

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    return datetime.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    return dt_obj.strftime(time_format)

def remove_server_num(f_name,suffix='.csv',serverNum=True):
    # remove server # from file name
    # eg. '2019-10-01-01-00-00-3.csv' > '2019-10-01-01-00-00'
    if "temp/" in f_name:
        f_name = f_name[5:]
    if serverNum:
        return '-'.join(f_name.split(suffix)[0].split('-')[:-1])
    else:
        return f_name.split(suffix)[0]

def list_s3_files(dir="serverpool", bucket='maxwell-insight'):
    dir += "/"
    conn = client('s3')
    list_of_files = [key['Key'].replace(dir,"",1) for key in conn.list_objects(Bucket=bucket, Prefix=dir)['Contents']]
    tmp = []
    for f in list_of_files:
        if 'temp/' in f or not f: continue
        tmp.append(f)
    return tmp

def folder_time_range(lof, time_format='%Y-%m-%d-%H-%M-%S', suffix=".csv",serverNum=True):
    # returns datetime.datetime objects
    file_times = []
    for f_name in lof:
        try:
            t = remove_server_num(f_name, suffix, serverNum)
            t = str_to_datetime(t, time_format)
            file_times.append(t)
        except Exception as e:
            print (e)
    if file_times:
        return min(file_times), max(file_times)
    else:
        # if time_format == '%Y-%m-%d-%H-%M-%S':
        return str_to_datetime("2019-09-30-23-59-00"), str_to_datetime("2019-09-30-23-59-00")
        # elif time_format == '%Y-%m-%d':
        #     return str_to_datetime("2019-09-30"), str_to_datetime("2019-09-30")

def remove_s3_file(bucket, src_dir, prefix):
    # bucket = 'maxwell-insight'
    # src_dir = 'serverpool/'
    # dst_dir = 'spark-processed/'
    os.system(f's3cmd rm s3://{bucket}/{src_dir}{prefix}*.csv')

def read_s3_to_df_bk(sql_c, spark, prefix):
    # read data from S3 ############################################################
    # for mini batches need to change this section into dynamical
    bucket = 'maxwell-insight'
    key = f"spark-processed/{prefix}*.csv"
    s3file = f's3a://{bucket}/{key}'
    return sql_c.read.csv(s3file, header=True)

def compress_csv(timeframe='hour'):
    sql_c, spark = spark_init()
    lof_pool = list_s3_files(dir="spark-processed", bucket = 'maxwell-insight')
    lof_zipped = list_s3_files(dir="csv-bookkeeping", bucket = 'maxwell-insight')
    max_processed_time = folder_time_range(lof_pool)[1]
    if timeframe == 'hour':
        tt_format = '%Y-%m-%d-%H'
        hour_diff = 1
    elif timeframe == 'day':
        tt_format = '%Y-%m-%d'
        hour_diff = 24
    max_zipped_time = folder_time_range(lof_zipped,tt_format,'.csv.gzip',False)[1]
    max_zipped_next = max_zipped_time + datetime.timedelta(hours=hour_diff)
    print ("Last processed file time label:", max_processed_time)
    print ("Last compressed file time label:", max_zipped_time)
    comp_f_name = next_prefix + ".csv.gzip"
    print (comp_f_name)
    if max_processed_time >= max_zipped_next:
        try:
            next_prefix = datetime_to_str(max_zipped_next, tt_format)
            df = read_s3_to_df_bk(sql_c, spark, prefix=next_prefix)

            df = df.withColumn('_c0', df['_c0'].cast('integer'))
            comp_f_name = next_prefix + ".csv.gzip"
            print (comp_f_name)

            # sort by index and compress
            df.orderBy('_c0')\
            .coalesce(1)\
            .write\
            .option("header", True)\
            .option("compression","gzip")\
            .csv(f"s3a://maxwell-insight/csv-bookkeeping/{comp_f_name}")
            remove_s3_file('maxwell-insight', 'spark-processed/', prefix=next_prefix)

        except Exception as e:
            print (e)
    else:
        print ("Not enough time has passed since last compression.")
        print (f"Currently compressed 1-{timeframe} starting from {max_zipped_time}")
        print (f"Currently newly processed files up until {max_processed_time}")
        return

if __name__ == "__main__":
    compress_csv()
