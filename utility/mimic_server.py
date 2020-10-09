import os

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'
os.system(f's3cmd mv s3://{bucket}/{src_dir}* s3://{bucket}/{dst_dir}')
