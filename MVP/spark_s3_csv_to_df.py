from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, substring, unix_timestamp, concat, lit, lpad
import datetime
import time

def timeConverter(timestamp):
    time_tuple = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.000").timetuple()
    timevalue = int(time.mktime(time_tuple)) # convert to int here
    return timevalue

def main():
    conf = SparkConf().setAppName("read_csv").setMaster("local")
    # define data location on S3 ###################################################
    region = 'us-east-2'
    bucket = 'maxwell-insight'
    #key = '2019-Nov.csv'
    key = 'sample.csv'
    s3file = f's3a://{bucket}/{key}'
    ################################################################################

    # initialize spark session and spark context####################################
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    spark_session = spark.builder.getOrCreate()
    sql_c = SQLContext(sc)
    ################################################################################

    # other settings ###############################################################
    # timestep of specific item data to group by, in seconds, for plotting
    tstep = 3600
    start_time = "2019-11-01 00:00:00 UTC"
    time_format = '%Y-%m-%d %H:%M:%S %Z'
    # start time for time series plotting, I'll set this to a specific time for now
    t0 = int(time.mktime(datetime.datetime.strptime(start_time, time_format).timetuple()))
    ################################################################################

    # read csv file on s3 into spark dataframe
    df = sql_c.read.csv(s3file, header=True)
    #
    # # drop unused column
    # df = df.drop('_c0')

    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    df = df.withColumn(
        'timestamp', unix_timestamp(col("event_time"), 'yyyy-MM-dd HH:mm:ss z')
        ).select(['timestamp']+df.columns[:-1]).drop('event_time')

    # create new column consisting of product_id + "-" + number of timesteps from
    # start time t0. Rows belonging to same product_id and within same chunck of timestep
    # will be aggregated together.
    # Eg. Views of product_id 1001588 between 00:00:00 to 00:01:00 (tstep = 60)
    # will have new column value as 1001588-0000000006

    df = df.withColumn("time_period", ((df.timestamp - t0) / tstep).cast('integer'))
    df = df.withColumn("time_period", lpad(df.time_period,10,'0'))

    # merge, rename, sort new column
    # df = df.select([concat(col("product_id"), lit("-"), col("time_period"))] + df.columns )
    # df = df.withColumnRenamed(df.columns[0], "pid_timeperiod")
    # df = df.sort("pid_timeperiod")

    df1 = df.groupBy("product_id","time_period","event_type").count().sort("product_id","time_period")

    df2 = df1.withColumn('ccol',concat(df1['source'],lit('_cnt'))).groupby('region').pivot('ccol').agg(F.first('count')).fillna(0)

    df2.show(n=100, truncate=False)

if __name__ == "__main__":
    main()
