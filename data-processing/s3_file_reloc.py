import boto3
s3 = boto3.resource('s3')


for bucket in s3.buckets.all():
    print(bucket.name)

buckets = s3.Bucket('maxwell-insight')
# dst = s3.Bucket('maxwell-insight/spark-processed/')

for file in buckets.objects.all():
    print(file)
