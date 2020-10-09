import os
import datetime as dt
import sched, time

# s3 parameters
bucket = 'maxwell-insight'
src_dir = 'minicsv/'
dst_dir = 'serverpool/'

# send file parameters
num_servers = 4
t_start = dt.datetime(2019,10,1,0,0,0)
t_freq = dt.timedelta(minutes=1)
send_time_gap = 2 #seconds
t_format = "%Y-%m-%d-%H-%M-%S"

# scheduler
s = sched.scheduler(time.time, time.sleep)

# initialize time
t_curr = t_start

def move_s3_file(sc, t_curr):
    t_str = t_curr.strftime(t_format)
    for i in range(num_servers):
        print (f'Sending file: {t_str}-{i}.csv')
        os.system(f's3cmd cp s3://{bucket}/{src_dir}{t_str}-{i}.csv s3://{bucket}/{dst_dir}')
    t_curr += dt.timedelta(minutes=1)
    s.enter(send_time_gap, 1, move_s3_file, (sc,t_curr,))

if __name__ == "__main__":
    s.enter(send_time_gap, 1, move_s3_file, (s,t_start,))
    s.run()
