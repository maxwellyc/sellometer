import boto

c = boto.connect_s3()
src = c.get_bucket('maxwell-insight/serverpool')
dst = c.get_bucket('maxwell-insight/spark-processed')

for k in src.list():
    # copy stuff to your destination here
    dst.copy_key(k.key.name, src.name, k.key.name)
    # then delete the source key
    k.delete()
