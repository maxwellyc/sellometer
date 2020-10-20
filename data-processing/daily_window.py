from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
from pyspark.sql import functions as F
import time, datetime, os
import logging

def spark_init():
    # initialize spark session and spark context####################################
    conf = SparkConf().setAppName("daily_window")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sql_c = SQLContext(sc)
    return sql_c, spark

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    return datetime.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    return dt_obj.strftime(time_format)

def get_latest_time_from_sql_db(spark, suffix='minute', time_format='%Y-%m-%d %H:%M:%S', latest=True):
    # reads previous processed time in logs/min_tick.txt and returns next time tick
    # default file names and locations
    if latest:
        order = "DESC"
    else:
        order = "ASC"
    try:
        query = f"""
        (SELECT event_time FROM view_brand_{suffix}
        ORDER BY event_time {order}
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
        tick = df.select('event_time').collect()[0][0]
        return tick
    except Exception as e:
        tick = "2019-10-01 00:00:00"
        return str_to_datetime(tick,time_format)

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

def read_sql_to_df(spark, t0='2019-10-01 00:00:00', t1='2020-10-01 00:00:00',
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

def daily_window(sql_c, spark, events, dimensions):

    time_format = '%Y-%m-%d %H:%M:%S'
    curr_max = get_latest_time_from_sql_db(spark, suffix='minute')
    curr_min = get_latest_time_from_sql_db(spark, suffix='minute', latest=False)
    print (curr_min, curr_max)
    if (curr_max - curr_min).seconds < 60*60*24:
        return
    for evt in events:
        for dim in dimensions:
            # remove data from more than 24 hours away from t1 table
            cutoff = datetime_to_str(curr_max - datetime.timedelta(hours=24))
            df_0 = read_sql_to_df(spark,t0=cutoff,event=evt,dim=dim,suffix='minute')
            # rewrite minute level data back to t1 table
            write_to_psql(df_0, evt, dim, mode="overwrite", suffix='minute_temp')
            df_temp = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute_temp')
            write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')

if __name__ == "__main__":

    dimensions = ['product_id', 'brand', 'category_l3']#, 'category_l2', 'category_l3']
    events = ['purchase', 'view'] # test purchase then test view
    sql_c, spark = spark_init()
    daily_window(sql_c, spark, events, dimensions)
