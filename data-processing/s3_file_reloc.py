import boto3
s3 = boto3.resource('s3')


for bucket in s3.buckets.all():
    print(bucket.name)

src = s3.Bucket('maxwell-insight/serverpool/')
dst = s3.Bucket('maxwell-insight/spark-processed/')

# for k in src.objects.all():
#     print(k)
