import boto3
s3 = boto3.resource('s3')

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'

result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
for k in result:
    print (k)

# s3_resource.Object('maxwell-insight', "").copy_from(
#  CopySource=”path/to/your/object_A.txt”)
