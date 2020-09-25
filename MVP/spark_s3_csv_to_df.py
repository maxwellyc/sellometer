from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, substring, unix_timestamp, concat, lit, lpad

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
    sql_c = SQLContext(sc)
    ################################################################################

    # other settings ###############################################################
    # timestep of specific item data to group by, in seconds, for plotting
    tstep = 60
    # start time for time series plotting, I'll set this to a specific time for now
    t0 = unix_timestamp("2019-11-01 00:00:00 UTC", 'yyyy-MM-dd HH:mm:ss z')
    ################################################################################

    # read csv file on s3 into spark dataframe
    df = sql_c.read.csv(s3file, header=True)

    # drop unused column
    df = df.drop('_c0')

    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    df = df.withColumn(
        'timestamp', unix_timestamp(col("event_time"), 'yyyy-MM-dd HH:mm:ss z')
        ).select(['timestamp']+df.columns[:-1]).drop('event_time')

    # create new column consisting of product_id + "-" + number of timesteps from
    # start time t0. Rows belonging to same product_id and within same chunck of timestep
    # will be aggregated together.
    # Eg. Views of product_id 10000000001 between 00:00:00 to 00:01:00 will have new column
    # value to be 10000000001-000000

    df.show(n=100, truncate=False)

    df = df.withColumn("time_period", df.timestamp - t0)
    df = df.withColumn("time_period", lpad(df.timestamp,10,'0'))

    return 
    df = df.withColumn('pid_timeperiod',
    [concat(col("product_id"), lit("-"), lpad(df.timestamp - t0,10,0))])

    #df = df.select([concat(col("product_id"), lit("-"), col("timestamp"))] + df.columns )
    # df2 = df2.withColumnRenamed(df2.columns[0], "pid_timestamp")

    df = df.sort("pid_timeperiod")

    df.show(n=100, truncate=False)

if __name__ == "__main__":
    main()
