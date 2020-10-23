''' Aggregate data from minute-level datatable into hourly data and stores into
PostgreSQL DB, also generate ranking over the past hour (sliding window)
'''
import datetime as dt
import imp

# load self defined modules
UTIL = imp.load_source('UTIL', '/home/ubuntu/eCommerce/data-processing/utility.py')
PSF = imp.load_source('PSF', '/home/ubuntu/eCommerce/data-processing/processing_funcs.py')
CONFIG = imp.load_source('CONFIG', '/home/ubuntu/eCommerce/data-processing/CONFIG.py')


def rolling_rank(df_0, evt, dim, curr_min):
    ''' Aggregated data over past hour (rolling window) for the purpose of ranking,
        will create {evt}_{dim}_rank datatables.
        These tables support variable selection in Grafana UI.
        These were originally used to update past hour sales metrics as well,
        but has since been decommissioned.
    '''
    # filter to keep only data within past hour, be wary of the inclusiveness
    # of the time ranges defined in UTIL.select_time_window()
    df_rank = UTIL.select_time_window(df_0,
                                      start_tick=curr_min - dt.timedelta(minutes=59),
                                      end_tick=curr_min + dt.timedelta(minutes=1))

    # aggregate (GROUP BY dim) entires in the past hour
    gb = PSF.merge_df(df_rank, evt, dim, rank=True)

    # overwrite ranked dataframe into sql table
    UTIL.write_to_psql(gb, evt, dim, mode="overwrite", suffix='rank')
    return

def min_to_hour(df_0, start_tick, end_tick, evt, dim):
    ''' Process minute-level data in t1 datatables into hourly data in t2 datatables
    '''

    # perform floor operation on event_time to the nearest hour
    # for aggregation on the hour
    df_hour = PSF.compress_time(df_0, start_tick, end_tick, tstep=3600, from_csv=False)

    # group by product dimension and event_time so that all events
    # having the same timestamp (within same hour) are aggregated
    gb = PSF.merge_df(df_hour, evt, dim)

    # append groupby (dataframe) to t2 datatables and complete the process
    UTIL.write_to_psql(gb, evt, dim, mode="append", suffix='hour')
    return

def data_transport(spark, events, dimensions):
    ''' Performs the following:
        1. Generate ranking datatable.
        2. Generate t2 datatables (hourly) from t1 datatables (minute-level).

        For event_time in t2 datatables, curr_hour is an aggregated value for
        event_time between curr_hour - 1hr to curr_hour
        ie A 10:00:00 entry in t2 datatable is aggregated using events between
        09:00:00 - 09:59:00 of the t1 datatable.

        Read comments marked as IMPORTANT for more details on how hourly
        data points are aggregated.
    '''
    # read latest event_time in t1 datatable and t2 datatable
    curr_min = UTIL.get_latest_time_from_db(spark, suffix='minute')
    curr_hour = UTIL.get_latest_time_from_db(spark, suffix='hour')

    # ************************* IMPORTANT *************************
    # calculate difference in latest times in t1 and t2 table, this is to
    # inform whether a full hour of data is available and ready to be aggregated
    # latest hour to be stored in t2 datatable for the current process.
    # eg. IF curr_min = 10:01:00 && curr_hour = 08:00:00
    #     THEN hours_diff = 2
    #     THEN end_hour = 10:00:00
    # The process below will generate hourly data with either 09:00:00 or 10:00:00
    # event_time (timestamps) in the t2 datatables
    # ************************* IMPORTANT *************************
    hours_diff = (curr_min - curr_hour).seconds // 3600
    end_hour = curr_hour + dt.timedelta(hours=hours_diff)

    for evt in events:
        for dim in dimensions:

            # read dataframe between curr_min and curr_hour
            df_0 = UTIL.read_sql_to_df(spark, curr_hour, curr_min, evt, dim, 'minute')

            # generate ranking datatable
            rolling_rank(df_0, evt, dim, curr_min)

            # aggregate new hourly data only when at least one hour has passed
            # since the lastest hour in t2 datatables
            if curr_hour > end_hour:
                min_to_hour(df_0, curr_hour, end_hour, evt, dim)
    return


if __name__ == "__main__":
    SQL_C, SPARK = UTIL.spark_init()
    data_transport(SPARK, CONFIG.EVENTS, CONFIG.DIMENSIONS)
    SPARK.stop()
