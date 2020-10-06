from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
from pyspark.sql import functions as F
import time, datetime, os

def timeConverter(timestamp):
    time_tuple = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.000").timetuple()
    timevalue = int(time.mktime(time_tuple)) # convert to int here
    return timevalue

def main():

    # initialize spark session and spark context####################################
    conf = SparkConf().setAppName("read_csv").setMaster("local")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    # spark_session = spark.builder\
    # .appName("read_csv")\
    # .config("spark.driver.extraClassPath", "/usr/share/java/postgresql/postgresql-42.2.16.jre7.jar")\
    # .getOrCreate()
    sql_c = SQLContext(sc)
    ################################################################################

    # read data from S3 ############################################################
    # for mini batches need to change this section into dynamical
    region = 'us-east-2'
    bucket = 'maxwell-insight'
    # key = '2019-Oct.csv'
    key = 'sample.csv'
    s3file = f's3a://{bucket}/{key}'
    # read csv file on s3 into spark dataframe
    df = sql_c.read.csv(s3file, header=True)
    # drop unused column
    df = df.drop('_c0')

    df.show(n=50, truncate = False)
    ################################################################################

    # Datetime transformation #######################################################
    tstep = 60 # unit in seconds, timestamp will be grouped in steps with stepsize of t_step seconds
    start_time = "2019-11-01 00:00:00 UTC"
    time_format = '%Y-%m-%d %H:%M:%S %Z'
    # start time for time series plotting, I'll set this to a specific time for now
    # t0 = int(time.mktime(datetime.datetime.strptime(start_time, time_format).timetuple()))

    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    df = df.withColumn(
        'timestamp', F.unix_timestamp(F.col("event_time"), 'yyyy-MM-dd HH:mm:ss z')
        ).select(['timestamp']+df.columns[:-1]).drop('event_time')
    df.show(n=50, truncate = False)

    t0 = df.agg({"timestamp": "min"}).collect()[0][0]
    df = df.withColumn("time_period", ((df.timestamp - t0) / tstep).cast('integer'))
    # 
    # df1 = df.groupBy("product_id","time_period","event_type").count().sort("product_id","time_period")

    df2.show(n=50, truncate=False)

    # write dataframe to postgreSQL

    df2.write\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://10.0.0.6:5431/my_db")\
    .option("dbtable","event_count")\
    .option("user",os.environ['psql_username'])\
    .option("password",os.environ['psql_pw'])\
    .option("driver","org.postgresql.Driver")\
    .mode("overwrite")\
    .save()

    df2.show(n=30, truncate=False)


if __name__ == "__main__":
    main()
