''' Process backlog data (data from an earlier timer sent after some delay), this
module will go into the minute-level datatable and reaggregate results.
Currently this does not fix the hourly datatable, but will be included soon (1`0/23/2020)
'''
import datetime as dt
import imp

# load self defined modules
UTIL = imp.load_source('UTIL', '/home/ubuntu/eCommerce/data-processing/utility.py')
PSF = imp.load_source('PSF', '/home/ubuntu/eCommerce/data-processing/processing_funcs.py')
INGESTION = imp.load_source('INGESTION', '/home/ubuntu/eCommerce/data-processing/ingestion.py')
CONFIG = imp.load_source('CONFIG', '/home/ubuntu/eCommerce/data-processing/config.py')

def process_backlogs(events, dimensions):
    ''' Process backlog files sent from server,
        If server sent event logs with timestamp earlier than latest event
        in t1 datatable, these logs will be moved to a backlog folder on S3
        awaiting processing.
        Optimization 1:
        Identify min and max time in backlog files, this will be used to slice t1
        datatable, and only union & merge backlog dataframe with corrupted portion
        of the table to reduce computation.
        In the future, this will be optimized with SQL query to only process timestamps
        directly affected by backlogs.
    '''

    sql_c, spark = UTIL.spark_init("backlog_processing")
    # new_df (two layer dictionary) stores dataframes processed from backlog files
    # keys are [event][dimension]
    new_df = {}
    new_df = INGESTION.ingest(sql_c, spark, events, dimensions, src_dir='backlogs/')

    t_min = new_df['view']['brand'].agg({"event_time": "min"}).collect()[0][0]\
            - dt.timedelta(minutes=1)
    # t_max = new_df['view']['brand'].agg({"event_time": "max"}).collect()[0][0]\
    #            + dt.timedelta(minutes=1)

    for evt in events:
        for dim in dimensions:
            # read intact portion of t1 datatable before earliest timestamp in backlog
            df_intact = UTIL.read_sql_to_df(spark, t1=t_min, event=evt, dim=dim, suffix='minute')

            # read corrupt portion of t1 datatable into dataframe, to be merged with backlog
            df_corrupt = UTIL.read_sql_to_df(spark, t0=t_min+dt.timedelta(minutes=1),
                                             event=evt, dim=dim, suffix='minute')

            # union corrupt t1 dataframe with backlog datafarame
            df_new = df_corrupt.union(new_df[evt][dim])

            # Merge duplicate entries, see merge_df doc for detailed explanation
            df_new = PSF.merge_df(df_new, evt, dim)

            # union fixed portion of corrupted table with intact portion
            df_fixed = df_intact.union(df_new)

            # write to temporary table and read back to avoid erasing original table
            # prematurely
            UTIL.write_to_psql(df_fixed, evt, dim, mode="overwrite", suffix='minute_bl')
            df_temp = UTIL.read_sql_to_df(spark, event=evt, dim=dim, suffix='minute_bl')
            UTIL.write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')

    spark.stop()
    # after backlogs are processed, move file to processed folder on S3
    UTIL.move_s3_file('maxwell-insight', 'backlogs/', 'spark-processed/')
    return

if __name__ == "__main__":
    process_backlogs(CONFIG.EVENTS, CONFIG.DIMENSIONS)
