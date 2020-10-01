import boto3

s3 = boto3.resource('s3')

for bucket in s3.buckets.all():
    print(bucket.name)

data = open('test.jpg', 'rb')
s3.Bucket('maxwell-insight').put_object(Key='test.jpg', Body=data)
