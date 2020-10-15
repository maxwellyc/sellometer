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
    conf = SparkConf().setAppName("backlog_processing")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sql_c = SQLContext(sc)
    return sql_c, spark

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    return datetime.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    return dt_obj.strftime(time_format)

def move_s3_file(bucket, src_dir, dst_dir, f_name='*.csv'):
    # bucket = 'maxwell-insight'
    # src_dir = 'serverpool/'
    # dst_dir = 'spark-processed/'
    os.system(f's3cmd mv s3://{bucket}/{src_dir}{f_name} s3://{bucket}/{dst_dir}')

def get_next_time_tick_from_log(next=True, debug=False):
    # reads previous processed time in logs/min_tick.txt and returns next time tick
    # default file names and locations
    def_tick = "2019-09-30-23-59-00"
    time_fn = "min_tick.txt"
    f_dir = "logs"
    if time_fn in os.listdir(f_dir):
        f = open(f"{f_dir}/{time_fn}",'r')
        time_tick = f.readlines()[0].strip("\n")
    else:
        time_tick = def_tick
    # debug override
    if debug:
        time_tick = '2019-10-01-00-19-00'
    time_tick = str_to_datetime(time_tick)
    if next:
        time_tick += datetime.timedelta(minutes=1)
    time_tick = datetime_to_str(time_tick)
    return time_tick

def read_s3_to_df(sql_c, spark, bucket = 'maxwell-insight', src_dir='serverpool/'):
    ################################################################################
    # read data from S3 ############################################################
    s3file = f's3a://{bucket}/{src_dir}*.csv'
    # read csv file on s3 into spark dataframe
    df = sql_c.read.csv(s3file, header=True)
    # drop unused column
    df = df.drop('_c0')
    return df
    ################################################################################

def compress_time(df, tstep = 60, from_csv = True):
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
    df = df.withColumn("event_time", ((df.event_time - t0) / tstep).cast('integer') * tstep + t0)
    df = df.withColumn("event_time", F.from_utc_timestamp(F.to_timestamp(df.event_time), 'UTC'))

    return df
    ################################################################################

def clean_data(spark,df):
    # Data cleaning ################################################################

    # if missing category code, fill with category id.
    df = df.withColumn('category_code', F.coalesce('category_code','category_id'))
    # if missing brand, fill with product id
    df = df.withColumn('brand', F.coalesce('brand','product_id'))

    # category_code have different layers of category,
    # eg. electronics.smartphones and electronics.video.tv
    # we need to create 3 columns of different levels of categories
    # this function uniforms all category_code into 3 levels category.
    def fill_cat_udf(code):
        code = str(code)
        ss = code.split('.')
        if len(ss) == 3:
            return code
        elif len(ss) == 2:
            return code + '.' + ss[-1]
        elif len(ss) == 1:
            return code + '.' + code + '.' + code

    fill_cat = spark.udf.register("fill_cat", fill_cat_udf)

    # df = df.select(fill_cat(F.col("category_code")))

    df = df.withColumn("category_code", fill_cat(F.col("category_code")))

    split_col = F.split(F.col("category_code"),'[.]')

    df = df.withColumn('category_l1', split_col.getItem(0))
    df = df.withColumn('category_l2', split_col.getItem(1))
    df = df.withColumn('category_l3', split_col.getItem(2))

    # level 3 category (lowest level) will determine category type, no need for category_id anymore
    df = df.drop('category_id')
    df = df.drop('category_code')

    ################################################################################
    # create separate dataframe for view and purchase,
    # data transformation will be different for these two types of events.
    return df

def split_by_event(events, df):
    main_df = {}
    for evt in events:
        main_df[evt] = df.filter(df['event_type'] == evt)
        main_df[evt] = main_df[evt].drop('event_type')

        # if same user session viewed same product_id twice, even at differnt event_time, remove duplicate entry.
        # same user refreshing the page should not reflect more interest on the same product
        # need to be careful here as user can buy a product twice within the same session,
        # we should not remove duplicate on purchase_df
        if evt == 'view':
            main_df[evt] = (main_df[evt]
                .orderBy('event_time')
                .coalesce(1)
                .dropDuplicates(subset=['user_session','product_id']))

    return main_df

def group_by_dimensions(main_df, events, dimensions):
    main_gb = {}
    # total view counts per dimesion, total sales amount per dimension
    for evt in events:
        main_gb[evt] = {}
        for dim in dimensions:
            # total view counts per dimension, if product_id, also compute mean price
            # total $$$ amount sold per dimension, if product_id also compute count and mean
            if dim == 'product_id':
                if evt == 'view':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.count('price'),F.mean('price')))
                elif evt == 'purchase':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.sum('price'),F.count('price'),F.mean('price')))
            else:
                if evt == 'view':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.count('price')))
                elif evt == 'purchase':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.sum('price')))
    return main_gb


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
    # df = pd.read_sql_table(table_name, engine)
    df = spark.read \
        .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce") \
    .option("dbtable", table_name) \
    .option("user",os.environ['psql_username'])\
    .option("password",os.environ['psql_pw'])\
    .option("driver","org.postgresql.Driver")\
    .load()

    return df

def stream_to_minute(sql_c, spark, events, dimensions, src_dir):
    # initialize spark
    # read csv from s3
    df_0 = read_s3_to_df(sql_c, spark, src_dir=src_dir)
    if not df_0:
        print ("No files were read into dataframe")
        return
    # clean data
    df_0 = clean_data(spark, df_0)
    # compress time into minute granularity, used for live monitoring
    df_0 = compress_time(df_0, tstep = 60)
    # # compress time into hour granularity
    main_df = split_by_event(events, df_0)
    # groupby different product dimensions
    main_gb = group_by_dimensions(main_df, events, dimensions)
    return main_gb

def merge_df(df, event, dim):
    # when performing union on backlog dataframe and main dataframe
    # need to recalculate average values
    if dim == 'product_id':
        if event == 'view':
            df = df.withColumn('total_price', F.col('count(price)') * F.col('avg(price)'))
            df = df.groupby(dim, 'event_time').agg(F.sum('count(price)'), F.sum('total_price'))
            df = df.withColumnRenamed('sum(count(price))','count(price)')
            df = df.withColumn('avg(price)', F.col('sum(total_price)') / F.col('count(price)'))
            df = df.drop('sum(total_price)')
        elif event == 'purchase':
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

def process_backlogs(events, dimensions):
    # initialize spark
    sql_c, spark = spark_init()
    new_df = {}
    new_df = stream_to_minute(sql_c,spark, events, dimensions,src_dir='backlogs/')
    for evt in events:
        for dim in dimensions:
            print (evt, dim)
            df = read_sql_to_df(spark, event=evt, dim=dim,suffix='minute')
            df = df.union(new_df[evt][dim])
            df = merge_df(df, evt, dim)
            write_to_psql(df, evt, dim, mode="overwrite", suffix='minute_bl')
            df_temp = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute_bl')
            write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')

    move_s3_file('maxwell-insight', 'backlogs/', 'spark-processed/')

if __name__ == "__main__":
    dimensions = ['product_id', 'brand', 'category_l3']#, 'category_l2', 'category_l3']
    events = ['purchase', 'view'] # test purchase then test view
    process_backlogs(events, dimensions)
