import os
import datetime as dt
import sched, time

# s3 parameters
bucket = 'maxwell-insight'
src_dir = 'minicsv/'
dst_dir = 'serverpool/'

# send file parameters
num_servers = 4
t_start = dt.datetime(2019,10,1,3,45,0)
t_end = dt.datetime(2019,10,8,2,23,0)
t_freq = dt.timedelta(minutes=1)
t_gap = 30 #seconds
t_format = "%Y-%m-%d-%H-%M-%S"

# scheduler
s = sched.scheduler(time.time, time.sleep)

# initialize time
t_curr = t_start

def move_s3_file(sc, t_curr, t_start, t_end, t_gap):
    t_str = t_curr.strftime(t_format)
    print (t_str)
    for i in range(num_servers):
        os.system(f's3cmd cp s3://{bucket}/{src_dir}{t_str}-{i}.csv s3://{bucket}/{dst_dir}')
    t_curr += dt.timedelta(minutes=1)
    if t_curr > t_end: t_curr = t_start
    s.enter(t_gap, 1, move_s3_file, (sc,t_curr,t_start,t_end,t_gap,))

if __name__ == "__main__":
    s.enter(0, 1, move_s3_file, (s,t_curr,t_start,t_end,t_gap,))
    s.run()
