import time, datetime, os
from boto3 import client

def list_s3_files(dir="serverpool", bucket = 'maxwell-insight'):
    dir += "/"
    conn = client('s3')
    list_of_files = [key['Key'].replace(dir,"",1) for key in conn.list_objects(Bucket=bucket, Prefix=dir)['Contents']]
    return list_of_files

def collect_backlogs():
    # move backlogged files into backlogs folder on s3
    try:
        lof = list_s3_files(dir = 'backlogs')
        for f in lof:
            if ".csv" in f:
                return 'process_backlogs'
        return 'dummy_task'
    except Exception as e:
        print (e)
        return 'dummy_task'

if __name__ == "__main__":
    print (collect_backlogs())
