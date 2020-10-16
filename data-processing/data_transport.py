from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
from pyspark.sql import functions as F
import time, datetime, os, imp
import logging
# daily_window = imp.load_source('util', '/home/ubuntu/eCommerce/data-processing/daily_window.py')

def spark_init():
    # initialize spark session and spark context####################################
    conf = SparkConf().setAppName("data_transport")
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
        logging.info(f'Latest event time in table <purchase_product_id_{suffix}> is: {t_max}')
        return t_max
    except Exception as e:
        t_max = "2019-09-30 23:00:00"
        logging.info(e)
        logging.info(f'Using default time: {t_max}')
        return t_max

def remove_min_data_from_sql(df, curr_time, hours_window=24):
    cutoff = curr_time - datetime.timedelta(hours=hours_window)
    logging.info(f"Current time: {curr_time}, 24 hours cutoff time: {cutoff}")
    df_cut = df.filter(df.event_time > cutoff )
    return df_cut

def select_time_window(df, start_tick, end_tick, time_format='%Y-%m-%d %H:%M:%S'):
    logging.info(f"Selecting data between {start_tick} - {end_tick-datetime.timedelta(seconds=1)}")
    df1 = df.filter( (df.event_time < end_tick) & (df.event_time >= start_tick) )
    return df1

def compress_time(df, start_tick=None, end_tick=None, tstep = 3600, t_window=24, from_csv = True):
    # Datetime transformation #######################################################
    # tstep: unit in seconds, timestamp will be grouped in steps with stepsize of t_step seconds
    start_time = "2019-10-01-00-00-00"
    time_format = '%Y-%m-%d-%H-%M-%S'
    # start time for time series plotting, I'll set this to a specific time for now
    t0 = int(time.mktime(datetime.datetime.strptime(start_time, time_format).timetuple()))
    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    if from_csv:
        df = df.withColumn('event_time',
            F.unix_timestamp(F.col("event_time"), 'yyyy-MM-dd HH:mm:ss'))
    if end_tick:
        if not start_tick: start_tick = end_tick - datetime.timedelta(hours=t_windows)
        df = select_time_window(df, start_tick=start_tick, end_tick=end_tick)
    df = df.withColumn("event_time", ((df.event_time.cast("long") - t0) / tstep).cast('long') * tstep + t0)
    df = df.withColumn("event_time", F.from_utc_timestamp(F.to_timestamp(df.event_time), 'UTC'))
    # t_max = df.agg({"event_time": "max"}).collect()[0][0]

    return df

def merge_df(df, event, dim, rank = False):
    # when performing union on backlog dataframe and main dataframe
    # need to recalculate average values
    if rank:
        gb_cols = [dim]
    else:
        gb_cols = [dim, 'event_time']
    if dim == 'product_id':
        if event == 'view':
            df = df.withColumn('total_price', F.col('count(price)') * F.col('avg(price)'))
            df = df.groupby(gb_cols).agg(F.sum('count(price)'), F.sum('total_price'))
            df = df.withColumnRenamed('sum(count(price))','count(price)')
            df = df.withColumn('avg(price)', F.col('sum(total_price)') / F.col('count(price)'))
            df = df.drop('sum(total_price)')
        elif event == 'purchase':
            df = df.groupby(gb_cols).agg(F.sum('sum(price)'), F.sum('count(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
            df = df.withColumn('avg(price)', F.col('sum(price)') / F.col('count(price)'))
    else:
        if event == 'view':
            df = df.groupby(gb_cols).agg(F.sum('count(price)'))
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
        elif event == 'purchase':
            df = df.groupby(gb_cols).agg(F.sum('sum(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')
    return df

def write_to_psql(df, event, dim, mode, suffix):
    # write dataframe to postgreSQL
    # suffix can be 'hour', 'minute', 'rank', this is used to name datatables
    logging.info(f"{mode} table {event}_{dim}_{suffix}")
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

def print_df_time_range(df, evt="",dim=""):
    tm0 = df.agg({"event_time": "min"}).collect()[0][0]
    tm1 = df.agg({"event_time": "max"}).collect()[0][0]
    logging.info(f"{evt}, {dim}, minute DB time range: {tm0}, {tm1}")

def min_to_hour(sql_c, spark, events, dimensions, verbose=False):

    time_format = '%Y-%m-%d %H:%M:%S'
    curr_min = str_to_datetime(get_latest_time_from_sql_db(spark, suffix='minute'), time_format)
    curr_hour = str_to_datetime(get_latest_time_from_sql_db(spark, suffix='hour'), time_format)
    hours_diff = (curr_min - curr_hour).seconds // 3600
    end_hour = curr_hour + datetime.timedelta(hours=hours_diff)
    logging.info(f"Current time in minute level table: {curr_min}")
    logging.info(f"Storing hourly data between: {curr_hour} and {end_hour}")
    for evt in events:
        for dim in dimensions:
            # read min data from t1 datatable
            logging.info("Ranking:")
            df_0 = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute')
            if verbose:
                logging.info("Ranking read in:")
                print_df_time_range(df_0,evt,dim)
            # slice 3600 second of dataframe for ranking purpose
            # rank datatable is a dynamic sliding window and updates every minute
            df_rank = select_time_window(df_0,
            start_tick=curr_min-datetime.timedelta(hours=1), end_tick=curr_min )
            if verbose:
                logging.info("Ranking selected time window:")
                print_df_time_range(df_rank,evt,dim)

            gb = merge_df(df_rank, evt, dim, rank=True)
            # store past hour data in rank table for ranking
            write_to_psql(gb, evt, dim, mode="overwrite", suffix='rank')
            # compress hourly data into t2 datatable only when integer hour has passed
            # since last hourly datapoint
            logging.info("Ranking complete! \n")
            if curr_min > end_hour:
                logging.info(f"++++++++Storing hourly data: {evt}_{dim}_hour")
                df_hour = compress_time(df_0, start_tick=curr_hour+datetime.timedelta(hours=1),
                end_tick=end_hour, tstep=3600, from_csv=False)
                if verbose:
                    print_df_time_range(df_hour,evt,dim)

                gb = merge_df(df_hour, evt, dim)
                # append temp table into t2 datatable
                write_to_psql(gb, evt, dim, mode="append", suffix='hour')
                logging.info(f"Hourly data appended for {curr_hour+datetime.timedelta(hours=1)}\
                 - {end_hour-datetime.timedelta(seconds=1)}")

            # merge events that have same product_id & event_time, sometimes 2 entries
            # can enter due to backlog or spark process only loaded partial data of that minute_
            # This process is identical as checking backlog, without the union part.
            # using multiple variables to prevent writing to original dataframe and causing error
            # read data from main datatable
            # df_pd = read_sql_to_df(spark, event=evt, dim=dim,suffix='minute')
            # data_time_merger(df_pd, spark, evt, dim, verbose)

def data_time_merger(df_pd, spark,evt, dim, verbose=False):

    if verbose:
        logging.info("df periodically crop just read")
        print_df_time_range(df_pd,evt,dim)

    # try to merge entries with duplicate product_id & event_time
    df_pd2 = merge_df(df_pd, evt, dim)
    if verbose:
        logging.info("df periodically crop just merged")
        print_df_time_range(df_pd2,evt,dim)

    # store merged dataframe to temporay datatable
    write_to_psql(df_pd2, evt, dim, mode="overwrite", suffix='minute_temp')

    # read from temporary datatable
    df_temp = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute_temp')

    # overwrite main datatable
    write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')

    if verbose:
        df_f = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute')
        logging.info("df periodically crop final stored")
        print_df_time_range(df_f,evt,dim)

if __name__ == "__main__":

    dimensions = ['product_id', 'brand', 'category_l3']#, 'category_l2', 'category_l3']
    events = ['purchase', 'view'] # test purchase then test view
    sql_c, spark = spark_init()
    min_to_hour(sql_c, spark, events, dimensions,verbose=False)
    # daily_window.daily_window(sql_c, spark, events, dimensions)
