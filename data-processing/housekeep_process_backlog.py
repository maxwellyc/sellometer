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

def read_s3_to_df(sql_c, spark, bucket = 'maxwell-insight', src_dir='serverpool/' ,read_time_tick = True):
    ################################################################################
    # read data from S3 ############################################################
    if read_time_tick:
        time_tick = get_next_time_tick_from_log()
    else:
        time_tick = ""
    print (f"Spark Cluster processing {time_tick} file batch")
    key = f'{src_dir}{time_tick}-*.csv'
    s3file = f's3a://{bucket}/{key}'
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
    t_max = df.agg({"event_time": "max"}).collect()[0][0]
    return df
    ################################################################################

def clean_data(df):
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

    # df.show(n=50)
    ################################################################################
    # create separate dataframe for view and purchase,
    # data transformation will be different for these two types of events.
    return df

def split_by_event(df):

    purchase_df = df.filter(df['event_type'] == 'purchase')
    view_df = df.filter(df['event_type'] == 'view')

    purchase_df = purchase_df.drop('event_type')
    view_df = view_df.drop('event_type')

    # if same user session viewed same product_id twice, even at differnt event_time, remove duplicate entry.
    # same user refreshing the page should not reflect more interest on the same product
    # need to be careful here as user can buy a product twice within the same session,
    # we should not remove duplicate on purchase_df
    view_df = (view_df
        .orderBy('event_time')
        .coalesce(1)
        .dropDuplicates(subset=['user_session','product_id'])
    )

    return view_df, purchase_df

def group_by_dimensions(view_df, purchase_df, dimensions):

    view_dims, purchase_dims = {}, {}
    # total view counts per dimesion, total sales amount per dimension
    for dim in dimensions:
        # total view counts per dimension, if product_id, also compute mean price
        # total $$$ amount sold per dimension, if product_id also compute count and mean
        if dim == 'product_id':
            view_dims[dim] = (view_df.groupby(dim, 'event_time')
                                .agg(F.count('price'),F.mean('price')))
            purchase_dims[dim] = (purchase_df.groupby(dim, 'event_time')
                                .agg(F.sum('price'),F.count('price'),F.mean('price')))
        else:
            view_dims[dim] = (view_df.groupby(dim, 'event_time')
                                .agg(F.count('price')))
            purchase_dims[dim] = (purchase_df.groupby(dim, 'event_time')
                                .agg(F.sum('price')))

        view_dims[dim]
        purchase_dims[dim]

    return view_dims, purchase_dims

def write_to_psql(view_dims, purchase_dims, dimensions, mode, timescale="minute"):
# write dataframe to postgreSQL
    for dim in dimensions:
        view_dims[dim].write\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce")\
        .option("dbtable","view_" + dim + f"_{timescale}")\
        .option("user",os.environ['psql_username'])\
        .option("password",os.environ['psql_pw'])\
        .option("driver","org.postgresql.Driver")\
        .mode(mode)\
        .save()

        purchase_dims[dim].write\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce")\
        .option("dbtable","purchase_" + dim + f"_{timescale}")\
        .option("user",os.environ['psql_username'])\
        .option("password",os.environ['psql_pw'])\
        .option("driver","org.postgresql.Driver")\
        .mode(mode)\
        .save()

def read_sql_to_df(engine, event='purchase', dimension='product_id',
time_gran='minute', group=False):
    table_name = "_".join([event, dimension, time_gran])
    df = pd.read_sql_table(table_name, engine)
    if not group:
        return df
    else:
        df_gb = df.groupby(by=[dimension]).sum()
        return df, df_gb

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

def merge_df(df, event, dim):
    # when performing union on backlog dataframe and main dataframe
    # need to recalculate average values

    if dim == 'product_id':
        if event == 'view':
        # view_dims[dim] = (view_df.groupby(dim, 'event_time')
        #                     .agg(F.count('price'),F.mean('price')))
            df = df.withColumn('total_price', F.col('count(price)') * F.col('mean(price)'))
            df = df.groupby(dim, 'event_time').agg(F.count('count(price)'), F.sum('total_price'))
            df = df.withColumnRenamed('count(count(price))','count(price)')
            df = df.withColumn('mean(price)', F.col('sum(total_price)') / F.col('count(price)'))
            df.drop('sum(total_price)')
            df.show(10)
        elif event == 'purchase':
            # purchase_dims[dim] = (purchase_df.groupby(dim, 'event_time')
            #                 .agg(F.sum('price'),F.count('price'),F.mean('price')))
            df = df.groupby(dim, 'event_time').agg(F.sum('sum(price)'), F.sum('count(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
            df = df.withColumn('mean(price)', F.col('sum(price)') / F.col('conut(price)'))
            df.show(10)
    else:
        if event == 'view':
            df = df.groupby(dim, 'event_time').agg(F.sum('count(price)'))
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
            df.show(10)
        elif event == 'purchase':
            df = df.groupby(dim, 'event_time').agg(F.sum('sum(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')

    return df

if __name__ == "__main__":
    dimensions = ['product_id', 'brand', 'category_l1', 'category_l2', 'category_l3']
    engine = create_engine(f"postgresql://{os.environ['psql_username']}:{os.environ['psql_pw']}@10.0.0.5:5431/ecommerce")
    new_df, main_df = {}, {'view':{}, 'purchase':{}}
    new_df['view'], new_df['purchase'] = spark_process(src_dir='backlogs/',read_time_tick=False)
    for evt in main_df:
        for dim in dimensions:
            main_df[evt][dim] = read_sql_to_df(engine, event=evt, dimension=dim,
             time_gran='minute', group=False)
            main_df[evt][dim] = main_df[evt][dim].union(new_df[evt][dim])
            main_df[evt][dim] = merge_df(main_df[evt][dim], evt, dim)



    write_to_psql(view_dim, purchase_dim, dimensions, mode = "overwrite", timescale="minute")
