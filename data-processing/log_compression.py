import datetime, logging, imp

# load self defined modules
util = imp.load_source('util', '/home/ubuntu/eCommerce/data-processing/utility.py')
config = imp.load_source('config', '/home/ubuntu/eCommerce/data-processing/config.py')

def logs_compression(compress_block='hour'):
    ''' Combine raw csv files and compress (gzip) into one single file.
        {compress_step} can be 'hour' or 'day' for compressing files into
        block size of one hour or one day.
    '''

    # list of files in spark-processed folder on AWS S3, these will be gzipped
    lof_pool = util.list_s3_files(dir="spark-processed")

    # list of file in csv-bookkepping folder on AWS S3, these were already gzipped
    lof_zipped = util.list_s3_files(dir="csv-bookkeeping")

    # latest time of files inside spark-processed folder
    max_processed_time = util.folder_time_range(lof_pool)[1]

    # Determined by whether compress csv happens hourly or daily
    if compress_block == 'hour':
        tt_format, hour_diff = '%Y-%m-%d-%H', 1
    elif compress_block == 'day':
        tt_format, hour_diff = '%Y-%m-%d', 24
    else:
        logging.info("Wrong input for compress_step, only supports 'hour' or 'day' ")
        return

    # latest time of files inside csv-bookkeeping folder
    max_zipped_time = util.folder_time_range(lof_zipped,tt_format,'.csv.gzip',False)[1]

    # the file name prefix (and thus time) of the next gzip file
    max_zip_next = max_zipped_time + datetime.timedelta(hours=hour_diff)

    # the available processed files needs to be {hour_diff} away since the latest
    # time of files inside csv-bookkeeping folder, this means all files needed
    # to create {max_zip_next}.csv.gzip is present and we can move forward
    if max_processed_time > max_zip_next + datetime.timedelta(hours=hour_diff):
        try:
            sql_c, spark = util.spark_init("logs_compression")

            # prefix of current gzip file name
            prefix = util.datetime_to_str(max_zip_next, tt_format)

            # read files that needed to be compressed into spark dataframe
            df = util.read_s3_to_df(sql_c, spark, src_dir='spark-processed/', prefix=prefix)

            # for some reason the index for each row is in string format, need to
            # change to int for sorting
            df = df.withColumn('_c0', df['_c0'].cast('integer'))

            # sort by index and gzip
            df.orderBy('_c0')\
            .coalesce(1)\
            .write\
            .option("header", True)\
            .option("compression","gzip")\
            .csv(f"s3a://maxwell-insight/csv-bookkeeping/{prefix}.csv.gzip")

            # remove csv files already compressed from <spark-processed> on AWS S3
            remove_s3_file('maxwell-insight', 'spark-processed/', prefix=next_prefix)

        except Exception as e:
            return

    spark.stop()
    return

if __name__ == "__main__":
    compress_csv(config.compress_block)
