import datetime, imp

# load self defined modules
util = imp.load_source('util', '/home/ubuntu/eCommerce/data-processing/utility.py')

def daily_window(events, dimensions, keep_hours=24):

    curr_max = util.get_latest_time_from_db(spark, suffix='minute')
    curr_min = util.get_latest_time_from_db(spark, suffix='minute', max_time=False)
    # only trigger daily window process if more than 26 hours is in t1 datatable
    # we don't want this expensive process being called every hour.
    if curr_min + datetime.timedelta(hours=26) > curr_max:
        return
    sql_c, spark = util.spark_init("daily_window")
    for evt in events:
        for dim in dimensions:
            # remove data from more than 24 hours away from t1 table
            cutoff = util.datetime_to_str(curr_max - datetime.timedelta(hours=keep_hours))
            df_0 = util.read_sql_to_df(spark,t0=cutoff,event=evt,dim=dim,suffix='minute')
            # write to temporary table and read back to avoid erasing original table
            # prematurely
            util.write_to_psql(df_0, evt, dim, mode="overwrite", suffix='minute_temp')
            df_temp = util.read_sql_to_df(spark,event=evt,dim=dim,suffix='minute_temp')
            util.write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')

if __name__ == "__main__":

    dimensions = ['product_id', 'brand', 'category_l3']#, 'category_l2', 'category_l3']
    events = ['purchase', 'view']
    daily_window(events, dimensions)
