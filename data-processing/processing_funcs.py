''' This module contains main (Spark) processing functions that transform
Spark DataFrame and are reused frequently in ingestion.py, data_transport.py,
and backlog_processing.py
'''

import time
import datetime as dt
import imp
from pyspark.sql import functions as F

# load self defined modules
UTIL = imp.load_source('UTIL', '/home/ubuntu/eCommerce/data-processing/utility.py')

def compress_time(df, start_tick=None, end_tick=None, tstep=3600, from_csv=True):
    ''' Performs time compression, "compress" time by flooring the real
        event_time to the nearest minute or hour (smaller value), eg:
            10:15:26 ---(tstep=60)---> 10:15:00
            10:15:26 ---(tstep=3600)-> 10:00:00
        Once the event_time is in minute or hour granularity, we can then perform
        GROUP BY on different product dimensions to acquire aggregated result for
        the entire minute or hour etc.

        Returns dataframe object.
    '''

    # t0 can be any time before event_time of the current dateset, this is merely
    # used to compute the difference in seconds to the event's timestamp
    # It is a integer representing seconds away from epoch time.
    t0 = int(time.mktime(UTIL.str_to_datetime("2019-10-01-00-00-00").timetuple()))

    # If read directly from csv, the event_time is of string type (VARCHAR)
    # here it is converted into timestamps if from_csv == True
    if from_csv:
        df = df.withColumn('event_time', F.unix_timestamp(F.col("event_time"),
                                                          'yyyy-MM-dd HH:mm:ss'))

    # If start_tick and / or end_tick is provided when calling this function,
    # the dataframe will be filtered based on these timestamps accordingly
    if end_tick:
        if not start_tick:
            start_tick = end_tick - dt.timedelta(hours=24)
        df = UTIL.select_time_window(df, start_tick=start_tick, end_tick=end_tick)

    # The "compression" part by flooring time to nearest minute or hour etc.
    df = df.withColumn("event_time",
                       ((df.event_time.cast("long") - t0) / tstep).cast('long')\
                       * tstep + t0 + tstep)

    # Convert integer value timestamp into datetime.datetime object
    df = df.withColumn("event_time", F.to_timestamp(df.event_time))

    return df

def clean_data(spark, df):
    ''' Performs data cleaning by:
        Filling null values in category_code and brand using other information.

        Returns dataframe object.
    '''

    # if missing category_code (string), fill with category_id (numeric).
    df = df.withColumn('category_code', F.coalesce('category_code', 'category_id'))

    # if missing brand (string), fill with product_id (numeric)
    df = df.withColumn('brand', F.coalesce('brand', 'product_id'))

    # category_code have different layers separated with periods -- '.'
    # eg. electronics.smartphones and electronics.video.tv
    # Need to create 3 columns for 3 levels of categories
    # This UDF standardizes all category_code into 3 category levels

    def fill_cat_udf(code):
        ''' Spark UDF that uniformly transforms all category_id into 3 layers'''
        code = str(code)
        ss = code.split('.')
        # if all 3 category levels already present in category_code
        # such as electronics.video.tv
        if len(ss) == 3:
            return code
        # if only 2 category levels present in category_code
        # such as electronics.smartphones, repeat last category level
        elif len(ss) == 2:
            return code + '.' + ss[-1]
        # if only 1 category level present in category_code
        # such as headphones, or category_id, repeat last category level
        elif len(ss) == 1:
            return code + '.' + code + '.' + code

    fill_cat = spark.udf.register("fill_cat", fill_cat_udf)

    # split category_code column into 3 columns representing 3 levels of category
    df = df.withColumn("category_code", fill_cat(F.col("category_code")))
    split_col = F.split(F.col("category_code"), '[.]')
    df = df.withColumn('category_l1', split_col.getItem(0))
    df = df.withColumn('category_l2', split_col.getItem(1))
    df = df.withColumn('category_l3', split_col.getItem(2))

    # category is uniquely determined by the 3 levels of category, remove original
    # category related columns
    df = df.drop('category_id')
    df = df.drop('category_code')

    return df

def split_event(events, df):
    ''' Split online events into separate dataframes, which will eventually be
        stored into separate datatables (t1 & t2) for optimal query.

        Returns dictionary of dataframe objects.
    '''
    main_df = {}
    for evt in events:
        main_df[evt] = df.filter(df['event_type'] == evt)
        main_df[evt] = main_df[evt].drop('event_type')

        # Remove likely duplicate view events
        # If during the same user session a product_id was viewed more than once
        # even at differnt event_time, this will only be counted as one view.
        if evt == 'view':
            main_df[evt] = (main_df[evt]
                            .orderBy('event_time')
                            .coalesce(1)
                            .dropDuplicates(subset=['user_session', 'product_id']))
    return main_df

def agg_by_dimensions(main_df, events, dimensions):
    ''' Aggregate values with same product dimension values, ie same product_id,
        or brand, or category
            For event_type == 'view', each entry in the main_df is a valid
            view event, each such event will have a 'price' column.
            Aggregation includes:
            COUNT on price, this counts the number of views
                IF product dimension == 'product_id', additional aggregation:
                MEAN of price, this is the average price of the product while
                being viewed, within the 1-minute period. This can be important
                for analysis on how price affect view -> cart / purchase conversion.

            For event_type == 'purchase', aggregation contains:
            SUM on price, this gives the total sales amount (GMV) within
            the 1-minute period.
                IF product dimension == 'product_id', additional aggregation:
                MEAN of price, this is the average price of the product being sold
                within the 1-minute period. This can be important for analysis on
                how price affect overall sales.

        Returns two-layer dictionary of groupby (dataframe) objects.
    '''

    main_gb = {}
    for evt in events:
        main_gb[evt] = {}
        for dim in dimensions:
            if dim == 'product_id':
                if evt == 'view':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                         .agg(F.count('price'), F.mean('price')))
                elif evt == 'purchase':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                         .agg(F.sum('price'), F.count('price'),
                                              F.mean('price')))
            else:
                if evt == 'view':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                         .agg(F.count('price')))
                elif evt == 'purchase':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                         .agg(F.sum('price')))
    return main_gb

def merge_df(df, event, dim, rank=False):
    ''' The merge process deals with scenarios when there exist two entries with
        same product_id (or brand, category etc.) and same event_time, this means
        the backlog data needs to be added to the t1 table entry
        If rank == True, this merge_df is used on {evt}_{dim}_rank data table which
        is a sliding window of aggregated values over the past hour, this table
        does not need event_time.

        Returns dataframe object.
    '''
    gb_cols = [dim]
    if not rank:
        gb_cols = gb_cols.append('event_time')

    if dim == 'product_id':
        if event == 'view':
            # To merge two or more entries that has column 'avg(price)', need to first
            # compute total_price = count * avg_price, then sum total_price and
            # total_count, then divide these two to get new average.
            df = df.withColumn('total_price', F.col('count(price)') * F.col('avg(price)'))
            df = df.groupby(gb_cols).agg(F.sum('count(price)'), F.sum('total_price'))
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
            df = df.withColumn('avg(price)', F.col('sum(total_price)') / F.col('count(price)'))
            df = df.drop('sum(total_price)')
        elif event == 'purchase':
            df = df.groupby(gb_cols).agg(F.sum('sum(price)'), F.sum('count(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
            df = df.withColumn('avg(price)', F.col('sum(price)') / F.col('count(price)'))
    else:
        if event == 'view':
            df = df.groupby(gb_cols).agg(F.sum('count(price)'))
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
        elif event == 'purchase':
            df = df.groupby(gb_cols).agg(F.sum('sum(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')
    return df


def duplicate_time_merger(df_pd, spark, evt, dim):
    ''' Merge events that have same product_id & event_time, sometimes 2 entries
        can enter due to backlog or spark process only loaded partial data of that minute_
        This process is identical as checking backlog, without the union part.
        using multiple variables to prevent writing to original dataframe and causing error
        read data from main datatable.
        This function will be used for later expansion of Sellometer and is currently
        inactive.
    '''
    df_pd = UTIL.read_sql_to_df(spark, event=evt, dim=dim, suffix='minute')
    # try to merge entries with duplicate product_id & event_time
    df_pd2 = merge_df(df_pd, evt, dim)
    # store merged dataframe to temporay datatable
    UTIL.write_to_psql(df_pd2, evt, dim, mode="overwrite", suffix='minute_temp')

    # read from temporary datatable
    df_temp = UTIL.read_sql_to_df(spark, event=evt, dim=dim, suffix='minute_temp')

    # overwrite main datatable
    UTIL.write_to_psql(df_temp, evt, dim, mode="overwrite", suffix='minute')
    return
