from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
from pyspark.sql import functions as F
import time, datetime

def timeConverter(timestamp):
    time_tuple = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.000").timetuple()
    timevalue = int(time.mktime(time_tuple)) # convert to int here
    return timevalue

def main():

    # define data location on S3 ###################################################
    region = 'us-east-2'
    bucket = 'maxwell-insight'
    # key = '2019-Oct.csv'
    key = 'sample.csv'
    s3file = f's3a://{bucket}/{key}'
    ################################################################################

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

    # other settings ###############################################################
    # timestep of specific item data to group by, in seconds, for plotting
    tstep = 3600*24
    start_time = "2019-10-01 00:00:00 UTC"
    time_format = '%Y-%m-%d %H:%M:%S %Z'
    # start time for time series plotting, I'll set this to a specific time for now
    t0 = int(time.mktime(datetime.datetime.strptime(start_time, time_format).timetuple()))
    ################################################################################

    # read csv file on s3 into spark dataframe
    df = sql_c.read.csv(s3file, header=True)
    #
    # drop unused column
    df = df.drop('_c0')

    df.show(n=50, truncate = False)

    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    df = df.withColumn(
        'timestamp', F.unix_timestamp(F.col("event_time"), 'yyyy-MM-dd HH:mm:ss z')
        ).select(['timestamp']+df.columns[:-1]).drop('event_time')

    # create new column consisting of product_id + "-" + number of timesteps from
    # start time t0. Rows belonging to same product_id and within same chunck of timestep
    # will be aggregated together.
    # Eg. Views of product_id 1001588 between 00:00:00 to 00:01:00 (tstep = 60)
    # will have new column value as 1001588-0000000006

    df = df.withColumn("time_period", ((df.timestamp - t0) / tstep).cast('integer'))
    df = df.withColumn("time_period", F.lpad(df.time_period,10,'0'))

    df1 = df.groupBy("product_id","time_period","event_type").count().sort("product_id","time_period")

    df2 = df1.withColumn('ccol',F.concat(df1['event_type'],
    F.lit('_cnt'))).groupby('product_id',"time_period").pivot('ccol').agg(F.first('count')).fillna(0)

    df2.show(n=50, truncate=False)

    # write dataframe to postgreSQL
    # my_writer = DataFrameWriter(df2)
    # url_connect = "jdbc:postgresql://10.0.0.6:5431"
    # table = "event_counts"
    # mode = "overwrite"
    # properties = {"user":"maxwell_insight", "password":"Insight2020CDESV"}
    # my_writer.jdbc(url_connect=url_connect, table=table, mode=mode, properties=properties)

    df2.write\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://10.0.0.6:5431/my_db")\
    .option("dbtable","event_count")\
    .option("user","maxwell_insight")\
    .option("password","Insight2020CDESV")\
    .option("driver","org.postgresql.Driver")\
    .save()



if __name__ == "__main__":
    main()
