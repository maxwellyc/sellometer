import boto3

bucket = 'maxwell-insight'
src_dir = 'serverpool/'
dst_dir = 'spark-processed/'

s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucket)

for o in my_bucket.objects.filter(Prefix=src_dir):
    f_name = o.key.split(src_dir)[-1]
    if not f_name: continue
    print(f_name)
    s3.Object(bucket, dst_dir + f_name ).copy_from(CopySource= bucket + "/" + o.key)
    s3.Object(bucket, o.key).delete()
