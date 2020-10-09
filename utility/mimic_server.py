import os

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'

#os.system(f's3cmd mv s3://{bucket}/{src_dir}*.csv s3://{bucket}/{dst_dir}')
os.system(f's3cmd cp s3://{bucket}/{src_dir}*.csv s3://{bucket}/{dst_dir}')
