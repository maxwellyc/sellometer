''' Module for processing (real-time) csv files containing online events and
store into minute-level datatable in PostgreSQL DB.
Also partitions backlog files and moving files to appropriate folders on AWS S3.
'''
import imp

# load self defined modules
UTIL = imp.load_source('UTIL', '/home/ubuntu/eCommerce/data-processing/utility.py')
PSF = imp.load_source('PSF', '/home/ubuntu/eCommerce/data-processing/processing_funcs.py')
CONFIG = imp.load_source('CONFIG', '/home/ubuntu/eCommerce/data-processing/config.py')


def partition_server_files(spark, src_dir="serverpool", bucket='maxwell-insight'):
    ''' Read list of files from AWS S3 serverpool, this folder contains files sent from
        server.
        Find current time in t1 datatable, if files in serverpool contains event from
        timestamp earlier than current time, indicates backlogs present, and proceed
        to move backlog files to backlog folder.
        In early iterations the log files were read directly from <serverpool> into Spark
        dataframe, this causes issues where the server is currently sending log files into
        <serverpool>, but the reading process has already started by spark, thus some files
        would be ommited, but will be moved into <spark-processed> without being actually
        processed.
        In this version a <processingpool> folder is introduced to remove this bug.
        Files movement in and out of <processingpool> is confined within this module and
        thus will not cause the race condition described above.
    '''
    lof = UTIL.list_s3_files(src_dir=src_dir, bucket=bucket)
    curr_time = UTIL.get_latest_time_from_db(spark, max_time=True)

    for f_name in lof:
        if ".csv" in f_name:
            tt_dt = UTIL.str_to_datetime(UTIL.remove_server_num(f_name))
            # if earlier than latest event_time already in t1 table, it is backlog
            if tt_dt < curr_time:
                UTIL.move_s3_file(bucket, 'serverpool/', 'backlogs/', f_name)
            # move logs ready to be processed into <processingpool>
            else:
                UTIL.move_s3_file(bucket, 'serverpool/', 'processingpool/', f_name)
    return

def ingest(sql_c, spark, events, dimensions, from_csv=True):
    ''' Performs listed tasks:
        1. Move files in <serverpool> into correct folders
        2. Read files in <processingpool> into Spark dataframe
        3. Clean dataframe, derive missing entries from other columns when possible
            Also removes duplicate viewing event from same user sessiom
        4. Group time into minute granuarity, raw events are in second granuarity,
        5. Split different type of events and store in separate t1 datatables.
        6. Aggregate on different product dimensions (product_id, brand, category etc.)
        7. Return groupby object dictionary

        Returns two-layer dictionary of groupby (dataframe) objects.
    '''
    # 1. redirect files
    partition_server_files(spark)

    # 2. read files into dataframe
    df_0 = UTIL.read_s3_to_df(sql_c, src_dir="processingpool/")
    if not df_0:
        return None

    # 3. clean data
    df_0 = PSF.clean_data(spark, df_0)

    # 4. compress time into minute granularity
    df_0 = PSF.compress_time(df_0, tstep=60, from_csv=from_csv)

    # 5. split event type
    main_df = PSF.split_event(events, df_0)

    # 6. Aggregate on different product dimensions
    main_gb = PSF.agg_by_dimensions(main_df, events, dimensions)

    return main_gb

def store_all_ingested(main_gb, events, dimensions, move_files=True):
    '''
        Stores groupby object into separate postgreSQL datatables by event types
        and product dimensions.
        Move processed files from <processingpool> to <spark-processed> on S3 for
        further compression.
    '''
    for evt in events:
        for dim in dimensions:
            UTIL.write_to_psql(main_gb[evt][dim], evt, dim, mode="append", suffix='minute')
    if move_files:
        UTIL.move_s3_file('maxwell-insight', 'processingpool/', 'spark-processed/')
    return

if __name__ == "__main__":
    SQL_C, SPARK = UTIL.spark_init("ingestion")
    GB = ingest(SQL_C, SPARK, CONFIG.EVENTS, CONFIG.DIMENSIONS)
    store_all_ingested(GB, CONFIG.EVENTS, CONFIG.DIMENSIONS)
    SPARK.stop()
