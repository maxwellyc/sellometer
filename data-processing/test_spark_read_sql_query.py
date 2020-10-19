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


def read_sql_to_df_time(spark,t0, t1, event='purchase', dim='product_id',suffix='minute'):
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

def test(spark, suffix='minute'):
    query = f"""
    (SELECT event_time FROM view_brand_{suffix}
    ORDER BY event_time DESC
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
    df.show()
    t_max = df.select('event_time').collect()[0][0]
    #t_max = df.agg({"event_time": "max"}).collect()[0][0]
    print (t_max)
    print (df)

if __name__ == "__main__":
    sql_c, spark = spark_init()
    t0 = '2019-10-01 00:12:00'
    t1 = '2019-10-01 01:10:00'
    test(spark, suffix='minute')
    #read_sql_to_df_time(spark,t0, t1, event='purchase', dim='product_id',suffix='minute')
