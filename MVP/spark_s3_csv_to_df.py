from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, substring, unix_timestamp, concat, lit

conf = SparkConf().setAppName("read_csv").setMaster("local")

region = 'us-east-2'
bucket = 'maxwell-insight'
key = '2019-Nov.csv'
s3file = f's3a://{bucket}/{key}'

sc = SparkContext(conf=conf)
spark = SparkSession(sc)
sql_c = SQLContext(sc)

df = sql_c.read.csv(s3file, header=True)
df = df.drop('_c0')

df = df.withColumn(
        'date_time', substring('event_time',0,19))

#df.show(n=50,truncate=False)

new_df = df.withColumn(
    'timestamp', unix_timestamp(col("date_time"), 'yyyy-MM-dd HH:mm:ss')
    ).select(['timestamp']+df.columns[:-1]).drop('date_time').drop('event_time')

new_df = new_df.select([concat(col("product_id"), lit("-"), col("timestamp"))] + new_df.columns )
new_df = new_df.withColumnRenamed(new_df.columns[0], "pid_timestamp")

new_df = new_df.sort("pid_timestamp")

new_df.show(n=100, truncate=False)
