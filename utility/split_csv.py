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
    print (df.head(10))
    # drop unused column
    ################################################################################


if __name__ == "__main__":
    main()
