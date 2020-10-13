from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
from pyspark.sql import functions as F
import time, datetime, os
from pyspark.sql.functions import pandas_udf, PandasUDFType
from boto3 import client

import psycopg2
import os
from sqlalchemy import create_engine

def spark_init():
    # initialize spark session and spark context####################################
    conf = SparkConf().setAppName("spark_check_backlog")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sql_c = SQLContext(sc)
    return sql_c, spark

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    return datetime.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    return dt_obj.strftime(time_format)

def return_tick(time_fn):
    try:
        f = open(f"logs/{time_fn}", 'r')
        return f.readlines()[0].strip("\n")
    except:
        return "2019-10-01-00-00-00"

def check_min_data_avail():
    # reads previous processed time in logs/min_tick.txt and returns next time tick
    # default file names and locations
    min_tick = return_tick("min_tick.txt")
    hour_tick = return_tick("hour_tick.txt")
    min_tick_dt = str_to_datetime(min_tick)
    hour_tick_dt = str_to_datetime(hour_tick)
    # <= so that all data of the hour is available. In case where eg.
    # 2019-10-02-10-00-00 has multiple entries but is only partially logged in
    if min_tick_dt <= hour_tick_dt + datetime.timedelta(hours=1):
        # no integer hour has passed since last min->hour process
        return None, None
    else:
        # have enough minute data to generate hour file up to previous hour + 1 hour
        return hour_tick_dt, hour_tick_dt+ datetime.timedelta(hours=1)

def remove_min_data_from_sql(df, hours_window = 24):
    # PSEUDO CODE, subtraction might not work
    t_max =  df.agg({"event_time": "max"}).collect()[0][0]
    print (t_max, type(t_max))
    cutoff = t_max - datetime.timedelta(hours=hours_window)
    df = df.filter(df.event_time > cutoff )
    return df

def write_time_tick_to_log(time_fn):
    # writes current processed time tick in logs/min_tick.txt for bookkeeping
    # default file names and locations
    time_fn = "min_tick.txt"
    f_dir = "logs"
    output = open(f"{f_dir}/{time_tick_fn}",'w')
    output.write(time_tick)
    output.close()
    return

def compress_time(df, start_tick="2019-10-01 00:00:00", end_tick="2019-10-01 00:00:00"
, tstep = 60, from_csv = True):
    # Datetime transformation #######################################################
    # tstep: unit in seconds, timestamp will be grouped in steps with stepsize of t_step seconds
    start_time = "2019-10-01 00:00:00"
    time_format = '%Y-%m-%d %H:%M:%S'
    # start time for time series plotting, I'll set this to a specific time for now
    t0 = int(time.mktime(datetime.datetime.strptime(start_time, time_format).timetuple()))
    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    if from_csv:
        df = df.withColumn(
            'event_time', F.unix_timestamp(F.col("event_time"), 'yyyy-MM-dd HH:mm:ss')
            )
    df = df.filter(df.event_time > cutoff )
    df = df.withColumn("event_time", ((df.event_time - t0) / tstep).cast('integer') * tstep + t0)
    df = df.withColumn("event_time", F.from_utc_timestamp(F.to_timestamp(df.event_time), 'UTC'))
    # t_max = df.agg({"event_time": "max"}).collect()[0][0]

    return df
    ################################################################################

def write_to_psql(df, event, dim, mode, timescale):
    # write dataframe to postgreSQL
    # timescale can be 'hour' or 'minute', this is used to name datatables
    df.write\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce")\
    .option("dbtable", f"{event}_{dim}_{timescale}")\
    .option("user",os.environ['psql_username'])\
    .option("password",os.environ['psql_pw'])\
    .option("driver","org.postgresql.Driver")\
    .mode(mode)\
    .save()
    return

def read_sql_to_df(engine, event='purchase', dimension='product_id',
time_gran='minute'):
    table_name = "_".join([event, dimension, time_gran])
    df = pd.read_sql_table(table_name, engine)
    return df

def spark_process(dimensions=['product_id'], src_dir='serverpool/',
read_time_tick=True, backlog_mode = False):
    #dimensions = ['product_id', 'brand', 'category_l1', 'category_l2', 'category_l3']
    # initialize spark
    sql_c, spark = spark_init()
    # read csv from s3
    df_0 = read_s3_to_df(sql_c, spark, src_dir, read_time_tick)
    # clean data
    df_0 = clean_data(df_0)
    # compress time into minute granularity
    df = compress_time(df_0, tstep = 60)

    # split by event type: view and purchase
    view_df, purchase_df = split_by_event(df)
    # groupby different product dimensions
    view_dim, purchase_dim = group_by_dimensions(view_df, purchase_df, dimensions)

    return view_dim, purchase_dim

    # write_to_psql(view_dim, purchase_dim, dimensions, mode = "overwrite", timescale="minute") # "append"

if __name__ == "__main__":
    dimensions = ['product_id', 'brand', 'category_l1', 'category_l2', 'category_l3']
    engine = create_engine(f"postgresql://{os.environ['psql_username']}:{os.environ['psql_pw']}@10.0.0.5:5431/ecommerce")
    new_df, main_df = {}, {'view':{}, 'purchase':{}}
    start_tick, end_tick = check_min_data_avail()
    if start_tick:
        for evt in main_df:
            for dim in dimesions:
                main_df[evt][dim] = read_sql_to_df(engine, event=evt, dimension=dim,
                 time_gran='minute')
                main_df[evt][dim] = compress_time(main_df[evt][dim],
                start_tick=start_tick, end_tick=end_tick,
                tstep=3600, from_csv=False)
                write_to_psql(main_df[evt][dim], evt, dim, mode="append", timescale='hour')
