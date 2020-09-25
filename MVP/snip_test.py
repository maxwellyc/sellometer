from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, substring, unix_timestamp, concat, lit, lpad

conf = SparkConf().setAppName("read_csv").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
sql_c = SQLContext(sc)

t0 = unix_timestamp("2019-11-01 00:00:00 UTC", 'yyyy-MM-dd HH:mm:ss zzz').cast("long")
#t1 = unix_timestamp("2019-11-01 00:00:00", 'yyyy-MM-dd HH:mm:ss')
print (t0 - 10)
