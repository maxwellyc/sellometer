from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
from pyspark.sql import functions as F
import time, datetime, os

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
        .option("dbtable", f'view_product_id_{suffix}') \
        .option("user",os.environ['psql_username'])\
        .option("password",os.environ['psql_pw'])\
        .option("driver","org.postgresql.Driver")\
        .load()
        t_max = df.agg({"event_time": "max"}).collect()[0][0]
        t_max = datetime_to_str(t_max,time_format)
        print (f'Latest event time in table <purchase_product_id_{suffix}> is: {t_max}')
        return t_max
    except Exception as e:
        t_max = "2019-10-01 00:00:00"
        print (e)
        print (f'Using default time: {t_max}')
        return t_max

def remove_min_data_from_sql(df, curr_time, hours_window=24):
    cutoff = curr_time - datetime.timedelta(hours=hours_window)
    print (f"Current time: {curr_time}, 24 hours cutoff time: {cutoff}")
    df_cut = df.filter(df.event_time > cutoff )
    print (curr_time, cutoff)
    return df_cut

def select_time_window(df, start_tick, end_tick, time_format='%Y-%m-%d %H:%M:%S'):
    print (f"Selecting data between {start_tick} - {end_tick}")
    df1 = df.filter( (df.event_time < end_tick) & (df.event_time >= start_tick) )
    return df1

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
    print (f"{mode} table {event}_{dim}_{suffix}")
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
    print (f"{evt}, {dim}, minute DB time range: {tm0}, {tm1}")

def daily_window(sql_c, spark, events, dimensions):

    time_format = '%Y-%m-%d %H:%M:%S'
    curr_min = str_to_datetime(get_latest_time_from_sql_db(spark, suffix='minute'), time_format)
    for evt in events:
        for dim in dimensions:
            # read min data from t1 datatable
            df_0 = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute')
            print ("First read-in from minute")
            print_df_time_range(df_0,evt,dim)

            # remove data from more than 24 hours away from t1 table
            df_cut = remove_min_data_from_sql(df_0, curr_min, hours_window = 24)
            print ("After cropping 24 hours window")
            print_df_time_range(df_cut,evt,dim)

            # rewrite minute level data back to t1 table
            write_to_psql(df_cut, evt, dim, mode="overwrite", suffix='minute_temp')
            df_temp = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute_temp')
            print ("Temp file being written")
            print_df_time_range(df_temp,evt,dim)

            write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')
            df_f = read_sql_to_df(spark,event=evt,dim=dim,suffix='minute')
            print ("Final write-in minute DB")
            print_df_time_range(df_f,evt,dim)

if __name__ == "__main__":

    dimensions = ['product_id', 'brand', 'category_l3']#, 'category_l2', 'category_l3']
    events = ['purchase', 'view'] # test purchase then test view
    sql_c, spark = spark_init()
    daily_window(sql_c, spark, events, dimensions)
