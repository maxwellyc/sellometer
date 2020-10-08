import boto3
s3_resource = boto3.resource(‘s3’)

s3_resource.Object("maxwell-insight", "spark-processed/").copy_from(
 CopySource='serverpool/*')
