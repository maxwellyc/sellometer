''' Maintains 24-hour window for minute-level PostgreSQL datatable.
    This will soon be replaced by direct SQL commands which will dramatically
    speed up the process. (10/23/2020)
'''
import datetime as dt
import imp

# load self defined modules
UTIL = imp.load_source('UTIL', '/home/ubuntu/eCommerce/data-processing/utility.py')
CONFIG = imp.load_source('CONFIG', '/home/ubuntu/eCommerce/data-processing/config.py')

def daily_window(events, dimensions, window_hours=24):
    '''
        Maintains 24-hour (default) window of t1 datatable by
        removing time more than 24-hours (default) away from curr_max
        Only triggers if curr_max - curr_min > 4 hours, we don't want this
        expensive operation to update every passing minute.
    '''
    sql_c, spark = UTIL.spark_init("daily_window")

    # get time range from current t1 datatable
    curr_max = UTIL.get_latest_time_from_db(spark, suffix='minute')
    curr_min = UTIL.get_latest_time_from_db(spark, suffix='minute', max_time=False)

    # if less than 30 hours in t1 datatable, ignore this process
    if curr_min + dt.timedelta(hours=30) > curr_max:
        return

    for evt in events:
        for dim in dimensions:
            # remove data from more than 24 hours away from t1 table
            cutoff = UTIL.datetime_to_str(curr_max - dt.timedelta(hours=window_hours),
                                          time_format='%Y-%m-%d %H:%M:%S')
            df_0 = UTIL.read_sql_to_df(spark, t0=cutoff, event=evt, dim=dim, suffix='minute')

            # write to temporary table and read back to avoid erasing original table
            # prematurely
            UTIL.write_to_psql(df_0, evt, dim, mode="overwrite", suffix='minute_temp')
            df_temp = UTIL.read_sql_to_df(spark, event=evt, dim=dim, suffix='minute_temp')
            UTIL.write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')

    spark.stop()
    return

if __name__ == "__main__":
    daily_window(CONFIG.EVENTS, CONFIG.DIMENSIONS)
