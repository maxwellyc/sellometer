import pandas as pd
import time, datetime, os

def main():

    ################################################################################

    # read data from S3 ############################################################
    # for mini batches need to change this section into dynamical
    region = 'us-east-2'
    bucket = 'maxwell-insight'
    key = '2019-Oct.csv'
    key = 'sample.csv'
    s3file = f's3a://{bucket}/{key}'
    # read csv file on s3 into spark dataframe
    df = pd.read_csv(s3file)
    ################################################################################

    # compress time, 60 second -> 1 second
    t_step = 60
    t_min = df['event_time'].min()
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['event_time'] = df['event_time'].values.astype(np.int64)  // 10 ** 9
    df['event_time'] = df['event_time'] - t_min
    df['event_time'] = df['event_time'] // t_step
    df['event_time'] = df['event_time'] + t_min
    df['event_time'] = pd.to_datetime(df['event_time'],unit='s')

    print (df.head(100))


if __name__ == "__main__":
    main()
