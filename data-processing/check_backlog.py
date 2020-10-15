import time, datetime, os
from boto3 import client

def list_s3_files(dir="serverpool", bucket = 'maxwell-insight'):
    dir += "/"
    conn = client('s3')
    list_of_files = [key['Key'].replace(dir,"",1) for key in conn.list_objects(Bucket=bucket, Prefix=dir)['Contents']]
    return list_of_files

def str_to_datetime(f_name, time_format='%Y-%m-%d-%H-%M-%S'):
    return datetime.datetime.strptime(f_name, time_format)

def datetime_to_str(dt_obj, time_format='%Y-%m-%d-%H-%M-%S'):
    return dt_obj.strftime(time_format)

def get_next_time_tick_from_log(next=True):
    # reads previous processed time in logs/min_tick.txt and returns next time tick
    # default file names and locations
    def_tick = "2019-10-01-00-00-00"
    time_fn = "min_tick.txt"
    f_dir = "logs"
    f = open(f"{f_dir}/{time_fn}",'r')
    if time_fn in os.listdir(f_dir):
        time_tick = f.readlines()[0].strip("\n")
    else:
        time_tick = def_tick
    time_tick = str_to_datetime(time_tick)
    if next:
        time_tick += datetime.timedelta(minutes=1)
    time_tick = datetime_to_str(time_tick)
    return time_tick

def get_latest_time_from_sql_db():
    # reads previous processed time in logs/min_tick.txt and returns next time tick
    # default file names and locations
    try:
        df = spark.read \
            .format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.0.5:5431/ecommerce") \
        .option("dbtable", 'purchase_product_id_minute') \
        .option("user",os.environ['psql_username'])\
        .option("password",os.environ['psql_pw'])\
        .option("driver","org.postgresql.Driver")\
        .load()
        t_max = datetime_to_str(str_to_datetime(df.agg({"event_time": "max"}).collect()[0][0]))
        print (f'Latest event time in DB is: {t_max}')
    except:
        t_max = "2019-10-01-00-00-00"
        print (f'Using default time: {t_max}')
    return t_max

def remove_server_num(f_name):
    # remove server # from file name
    # eg. '2019-10-01-01-00-00-3.csv' > '2019-10-01-01-00-00'
    return '-'.join(f_name.strip(".csv").split('-')[:-1])

def collect_backlogs():
    # move backlogged files into backlogs folder on s3
    try:
        bucket = 'maxwell-insight'
        src_dir = 'serverpool/'
        dst_dir = 'backlogs/'
        lof = list_s3_files()
        curr_time_tick = get_latest_time_from_sql_db()
        backlogs_exist = False
        for f_name in lof:
            if ".csv" in f_name:
                tt_dt = str_to_datetime(remove_server_num(f_name))
                if tt_dt < str_to_datetime(curr_time_tick):
                    backlogs_exist = True
                    print (f"Current time: {curr_time_tick} --- Backlog file: {f_name}")
                    os.system(f's3cmd mv s3://{bucket}/{src_dir}{f_name} s3://{bucket}/{dst_dir}')
        if backlogs_exist:
            return 'process_backlogs'
        else:
            return 'spark_live_process'
    except Exception as e:
        print (e)
        return 'spark_live_process'

if __name__ == "__main__":
    collect_backlogs()