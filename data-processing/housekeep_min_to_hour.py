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
    conf = SparkConf().setAppName("DT_tier_transfer").set("spark.sql.sources.partitionOverwriteMode","dynamic")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sql_c = SQLContext(sc)
    return sql_c, spark

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    return datetime.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    return dt_obj.strftime(time_format)

def get_latest_time_from_sql_db(spark, suffix='minute', time_format='%Y-%m-%d %H:%M:%S'):
    # reads previous processed time in logs/min_tick.txt and returns next time tick
    # default file names and locations
    try:
        df = spark.read \
            .format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce") \
        .option("dbtable", f'purchase_product_id_{suffix}') \
        .option("user",os.environ['psql_username'])\
        .option("password",os.environ['psql_pw'])\
        .option("driver","org.postgresql.Driver")\
        .load()
        t_max = df.agg({"event_time": "max"}).collect()[0][0]
        t_max = datetime_to_str(t_max,time_format)
        print (f'Latest event time in table <purchase_product_id_{suffix}> is: {t_max}')
        return t_max
    except:
        t_max = "2019-10-01 00:00:00"
        print (f'Using default time: {t_max}')
        return t_max


def remove_min_data_from_sql(df, curr_time, hours_window=24):
    cutoff = curr_time - datetime.timedelta(hours=hours_window)
    print (f"Current time: {curr_time}, 24 hours cutoff time: {cutoff}")
    df_cut = df.filter(df.event_time > cutoff )
    return df_cut

def select_time_window(df, start_tick, t_window=1, time_format='%Y-%m-%d %H:%M:%S'):
    df = df.filter( (df.event_time >= start_tick) &
    (df.event_time < start_tick + datetime.timedelta(hours=t_window)) )
    return df

def compress_time(df, t_window, start_tick, tstep = 60, from_csv = True ):
    # Datetime transformation #######################################################
    # tstep: unit in seconds, timestamp will be grouped in steps with stepsize of t_step seconds
    start_time = "2019-10-01-00-00-00"
    time_format = '%Y-%m-%d-%H-%M-%S'
    # start time for time series plotting, I'll set this to a specific time for now
    t0 = int(time.mktime(datetime.datetime.strptime(start_time, time_format).timetuple()))
    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    if from_csv:
        df = df.withColumn(
            'event_time', F.unix_timestamp(F.col("event_time"), 'yyyy-MM-dd HH:mm:ss')
            )
    if t_window:
        df = select_time_window(df, start_tick=start_tick, t_window=t_window)
    df = df.withColumn("event_time", ((df.event_time.cast("long") - t0) / tstep).cast('long') * tstep + t0)
    df = df.withColumn("event_time", F.from_utc_timestamp(F.to_timestamp(df.event_time), 'UTC'))
    # t_max = df.agg({"event_time": "max"}).collect()[0][0]

    return df

def merge_df(df, event, dim):
    # when performing union on backlog dataframe and main dataframe
    # need to recalculate average values
    if dim == 'product_id':
        if event == 'view':
        # view_dims[dim] = (view_df.groupby(dim, 'event_time')
        #                     .agg(F.count('price'),F.avg('price')))
            df = df.withColumn('total_price', F.col('count(price)') * F.col('avg(price)'))
            df = df.groupby(dim, 'event_time').agg(F.count('count(price)'), F.sum('total_price'))
            df = df.withColumnRenamed('count(count(price))','count(price)')
            df = df.withColumn('avg(price)', F.col('sum(total_price)') / F.col('count(price)'))
            df.drop('sum(total_price)')
        elif event == 'purchase':
            # purchase_dims[dim] = (purchase_df.groupby(dim, 'event_time')
            #                 .agg(F.sum('price'),F.count('price'),F.avg('price')))
            df = df.groupby(dim, 'event_time').agg(F.sum('sum(price)'), F.sum('count(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
            df = df.withColumn('avg(price)', F.col('sum(price)') / F.col('count(price)'))
    else:
        if event == 'view':
            df = df.groupby(dim, 'event_time').agg(F.sum('count(price)'))
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
        elif event == 'purchase':
            df = df.groupby(dim, 'event_time').agg(F.sum('sum(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')

    return df

def write_to_psql(df, event, dim, mode, suffix):
    # write dataframe to postgreSQL
    # suffix can be 'hour', 'minute', 'rank', this is used to name datatables
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

def read_sql_to_df(spark, event='purchase', dim='product_id',suffix='minute'):
    table_name = "_".join([event, dim, suffix])
    df = spark.read \
        .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce") \
    .option("dbtable", table_name) \
    .option("user",os.environ['psql_username'])\
    .option("password",os.environ['psql_pw'])\
    .option("driver","org.postgresql.Driver")\
    .load()

    return df

def min_to_hour(dimensions, events):
    # start_tick = check_min_data_avail()
    # if start_tick:
    sql_c, spark = spark_init()
    time_format = '%Y-%m-%d %H:%M:%S'
    curr_min = str_to_datetime(get_latest_time_from_sql_db(spark, suffix='minute'), time_format)
    curr_hour = str_to_datetime(get_latest_time_from_sql_db(spark, suffix='hour'), time_format)
    for evt in events:
        for dim in dimensions:
            # read min data from t1 datatable
            df_0 = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute')
            # remove data from more than 24 hours away from t1 datatable
            df_cut = remove_min_data_from_sql(df_0, curr_min, hours_window = 24)
            df_cut.show(50)
            # rewrite minute level data
            write_to_psql(df_cut, evt, dim, mode="overwrite", suffix='minute')

            # slice 3600 second of dataframe for ranking purpose
            # rank datatable is a dynamic sliding window and updates every minute
            df = select_time_window(df_0, start_tick=curr_min )
            # store past hour data in rank table for ranking
            write_to_psql(df, evt, dim, mode="overwrite", suffix='rank')

            # compress hourly data into t2 datatable only when integer hour has passed
            # since last hourly datapoint
            if curr_min > curr_hour + datetime.timedelta(hours=1):
                df = compress_time(df_0, t_window=3600, start_tick=curr_hour,
                tstep=3600, from_csv=False)
                gb = merge_df(df, evt, dim)
                # append temp table into t2 datatable
                write_to_psql(gb, evt, dim, mode="overwrite", suffix='hour')


if __name__ == "__main__":

    dimensions = ['product_id']#, 'brand', 'category_l1', 'category_l2', 'category_l3']
    events = ['purchase']#, 'view'] # test purchase then test view
    min_to_hour(dimensions, events)
