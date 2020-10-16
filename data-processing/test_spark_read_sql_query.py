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


def read_sql_to_df(spark, event='purchase', dim='product_id',suffix='minute'):
    table_name = "_".join([event, dim, suffix])
    query = "(SELECT * FROM " + table_name + ") as users"
    df = spark.read \
        .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce") \
    .option("dbtable", query) \
    .option("user",os.environ['psql_username'])\
    .option("password",os.environ['psql_pw'])\
    .option("driver","org.postgresql.Driver")\
    .load()

    df.show(100)
    return df

if __name__ == "__main__":
    sql_c, spark = spark_init()
    read_sql_to_df(spark, event='purchase', dim='product_id',suffix='hour')
