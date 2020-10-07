import pandas as pd
import time, datetime, os
import numpy as np
from io import StringIO # python3; python2: BytesIO
import boto3

def main(key = '2020-Feb'):
    key = key + ".csv"
    # key = 'sample.csv'
    ################################################################################
    # read csv file on s3 into spark dataframe
    region = 'us-east-2'
    bucket = 'maxwell-insight'
    s3file = "s3a://" + bucket + "/"+ key
    print (s3file)
    # read csv file on s3 into spark dataframe
    df = pd.read_csv(s3file)
    print (f'{time.asctime( time.localtime(time.time()) )}\n{key} read into pandas dataframe!')
    ################################################################################

    # compress time, 60 second -> 1 second
    t_step = 30 # unit in seconds, timestamp will be grouped in steps with stepsize of t_step seconds
    df['event_time'] = ((pd.to_datetime(df['event_time']) - pd.Timestamp("1970-01-01", tz='utc') )  / pd.Timedelta('1s'))
    t_min = pd.Timestamp("2019-10-01", tz='utc').value // 10**9
    df['event_time'] = df['event_time'] - t_min
    df['event_time'] = df['event_time'] // t_step
    df['event_time'] = (df['event_time'] + t_min).astype(np.int64)
    df['event_time'] = pd.to_datetime(df['event_time'], unit='s')#.dt.strftime("%Y-%m-%d %H:%M:%S")
    print (f'{time.asctime( time.localtime(time.time()) )}\nTime transformation complete\n')
    t0 = df['event_time'].min()
    t1 = t0 + pd.Timedelta(seconds=60)
    t_end = df['event_time'].max()

    while t0 < t_end:
        df_temp = df[ (df['event_time'] > t0) & (df['event_time'] < t1 )]
        for i in range(4):
            f_name = t0.strftime("%Y-%m-%d-%H-%M-%S") + '-' + str(i) + '.csv'
            print (f_name)
            df_i = df_temp.iloc[i::4, :]
            csv_buffer = StringIO()
            df_i.to_csv(csv_buffer)
            s3_resource = boto3.resource('s3')
            s3_resource.Object(bucket, "minicsv/" + f_name).put(Body=csv_buffer.getvalue())
        t0 += pd.Timedelta(seconds=60)
        t1  = t0 + pd.Timedelta(seconds=60)


if __name__ == "__main__":
    #for key in ["2019-Oct","2019-Nov","2019-Dec","2020-Jan","2020-Feb","2020-Mar","2020-Apr"]:
    main(key = "2019-Dec")
