''' Move files from an AWS S3 directory into another directory to mimic server
behavior, files are split into num_servers (determined by split_csv.py)
'''
import os
import datetime as dt
import sched
import time

# s3 parameters
BUCKET = 'maxwell-insight'
SRC_DIR = 'minicsv/'
DST_DIR = 'serverpool/'

# send file parameters
NUM_SERVERS = 4
T_START = dt.datetime(2019, 10, 1, 19, 16, 0)
T_END = dt.datetime(2019, 10, 8, 2, 23, 0)
T_GAP = 60 #seconds
T_FORMAT = "%Y-%m-%d-%H-%M-%S"

# scheduler
s = sched.scheduler(time.time, time.sleep)

# initialize time
T_CURR = T_START

def move_s3_file(sc, t_curr, t_start, t_end, t_gap):
    ''' Moves files between AWS S3 directories
    '''
    t_str = t_curr.strftime(T_FORMAT)
    print(t_str)
    for i in range(NUM_SERVERS):
        os.system(f's3cmd cp s3://{BUCKET}/{SRC_DIR}{t_str}-{i}.csv s3://{BUCKET}/{DST_DIR}')
    t_curr += dt.timedelta(minutes=1)
    if t_curr > t_end:
        t_curr = t_start
    s.enter(t_gap, 1, move_s3_file, (sc, t_curr, t_start, t_end, t_gap,))

if __name__ == "__main__":
    s.enter(0, 1, move_s3_file, (sc, T_CURR, T_START, T_END, T_GAP,))
    s.run()
