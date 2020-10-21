import datetime, imp

# load self defined modules
util = imp.load_source('util', '/home/ubuntu/eCommerce/data-processing/utility.py')
config = imp.load_source('config', '/home/ubuntu/eCommerce/data-processing/config.py')

def daily_window(events, dimensions, window_hours=24):
    '''
        Maintains 24-hour (default) window of t1 datatable by
        removing time more than 24-hours (default) away from curr_max
        Only triggers if curr_max - curr_min > 4 hours, we don't want this
        expensive operation to update every passing minute.
    '''
    # get time range from current t1 datatable
    curr_max = util.get_latest_time_from_db(spark, suffix='minute')
    curr_min = util.get_latest_time_from_db(spark, suffix='minute', max_time=False)

    # if less than 30 hours in t1 datatable, ignore this process
    if curr_min + datetime.timedelta(hours=30) > curr_max:
        return

    sql_c, spark = util.spark_init("daily_window")

    for evt in events:
        for dim in dimensions:
            # remove data from more than 24 hours away from t1 table
            cutoff = util.datetime_to_str(curr_max - datetime.timedelta(hours=window_hours))
            df_0 = util.read_sql_to_df(spark,t0=cutoff,event=evt,dim=dim,suffix='minute')

            # write to temporary table and read back to avoid erasing original table
            # prematurely
            util.write_to_psql(df_0, evt, dim, mode="overwrite", suffix='minute_temp')
            df_temp = util.read_sql_to_df(spark,event=evt,dim=dim,suffix='minute_temp')
            util.write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')
    return 
    spark.stop()

if __name__ == "__main__":
    daily_window(config.events, config.dimensions)
