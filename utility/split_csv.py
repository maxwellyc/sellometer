import pandas as pd
import time, datetime, os
import numpy as np
from io import StringIO # python3; python2: BytesIO
import boto3

def main():

    ################################################################################

    # read data from S3 ############################################################
    # for mini batches need to change this section into dynamical
    region = 'us-east-2'
    bucket = 'maxwell-insight'
    csv_buffer = StringIO()
    key = '2019-Oct.csv'
    #key = 'sample.csv'
    s3file = f's3a://{bucket}/{key}'
    # read csv file on s3 into spark dataframe
    df = pd.read_csv(s3file)
    ################################################################################

    # compress time, 60 second -> 1 second
    t_step = 60 # unit in seconds, timestamp will be grouped in steps with stepsize of t_step seconds
    df['event_time'] = ((pd.to_datetime(df['event_time']) - pd.Timestamp("1970-01-01", tz='utc') )  / pd.Timedelta('1s'))
    t_min = df['event_time'].min()
    df['event_time'] = df['event_time'] - t_min
    df['event_time'] = df['event_time'] // t_step
    df['event_time'] = (df['event_time'] + t_min).astype(np.int64)
    df['event_time'] = pd.to_datetime(df['event_time'], unit='s')#.dt.strftime("%Y-%m-%d %H:%M:%S")

    t0 = df['event_time'].min()
    t1 = t0 + pd.Timedelta(seconds=60)
    t_end = df['event_time'].max()

    while t0 < t_end:
        df_temp = df[ (df['event_time'] > t0) & (df['event_time'] < t1 )]
        for i in range(6):
            f_name = t0.strftime("%Y-%m-%d-%H-%M-%S") + '-' + str(i) + '.csv'
            df_i = df_temp.iloc[i::6, :]
            df_i.to_csv(csv_buffer)
            s3_resource = boto3.resource('s3')
            s3_resource.Object(bucket, "minicsv/" + f_name).put(Body=csv_buffer.getvalue())
        t0 += pd.Timedelta(seconds=60)
        t1  = t0 + pd.Timedelta(seconds=60)


if __name__ == "__main__":
    main()
