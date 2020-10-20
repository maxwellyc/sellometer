from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrameWriter
from pyspark.sql import functions as F
import time, datetime, os

def compress_time(df, start_tick=None, end_tick=None, tstep = 3600, t_window=24, from_csv = True):
    # Datetime transformation #######################################################
    # tstep: unit in seconds, timestamp will be grouped in steps with stepsize of t_step seconds
    start_time = "2019-10-01-00-00-00"
    time_format = '%Y-%m-%d-%H-%M-%S'
    # start time for time series plotting, I'll set this to a specific time for now
    t0 = int(time.mktime(datetime.datetime.strptime(start_time, time_format).timetuple()))
    # convert data and time into timestamps, remove orginal date time column
    # reorder column so that timestamp is leftmost
    if from_csv:
        df = df.withColumn('event_time',
            F.unix_timestamp(F.col("event_time"), 'yyyy-MM-dd HH:mm:ss'))
    if end_tick:
        if not start_tick: start_tick = end_tick - datetime.timedelta(hours=t_windows)
        df = select_time_window(df, start_tick=start_tick, end_tick=end_tick)
    df = df.withColumn("event_time",
    ((df.event_time.cast("long") - t0) / tstep).cast('long') * tstep + t0 + tstep)
    df = df.withColumn("event_time", F.to_timestamp(df.event_time))

    return df

def clean_data(spark,df):
    # Data cleaning ################################################################

    # if missing category code, fill with category id.
    df = df.withColumn('category_code', F.coalesce('category_code','category_id'))
    # if missing brand, fill with product id
    df = df.withColumn('brand', F.coalesce('brand','product_id'))

    # category_code have different layers of category,
    # eg. electronics.smartphones and electronics.video.tv
    # we need to create 3 columns of different levels of categories
    # this function uniforms all category_code into 3 levels category.
    def fill_cat_udf(code):
        code = str(code)
        ss = code.split('.')
        if len(ss) == 3:
            return code
        elif len(ss) == 2:
            return code + '.' + ss[-1]
        elif len(ss) == 1:
            return code + '.' + code + '.' + code

    fill_cat = spark.udf.register("fill_cat", fill_cat_udf)

    # df = df.select(fill_cat(F.col("category_code")))

    df = df.withColumn("category_code", fill_cat(F.col("category_code")))

    split_col = F.split(F.col("category_code"),'[.]')

    df = df.withColumn('category_l1', split_col.getItem(0))
    df = df.withColumn('category_l2', split_col.getItem(1))
    df = df.withColumn('category_l3', split_col.getItem(2))

    # level 3 category (lowest level) will determine category type, no need for category_id anymore
    df = df.drop('category_id')
    df = df.drop('category_code')

    ################################################################################
    # create separate dataframe for view and purchase,
    # data transformation will be different for these two types of events.
    return df

def split_event(events, df):
    main_df = {}
    for evt in events:
        main_df[evt] = df.filter(df['event_type'] == evt)
        main_df[evt] = main_df[evt].drop('event_type')

        # if same user session viewed same product_id twice, even at differnt event_time, remove duplicate entry.
        # same user refreshing the page should not reflect more interest on the same product
        # need to be careful here as user can buy a product twice within the same session,
        # we should not remove duplicate on purchase_df
        if evt == 'view':
            main_df[evt] = (main_df[evt]
                .orderBy('event_time')
                .coalesce(1)
                .dropDuplicates(subset=['user_session','product_id']))

    return main_df

def group_by_dimensions(main_df, events, dimensions):
    main_gb = {}
    # total view counts per dimesion, total sales amount per dimension
    for evt in events:
        main_gb[evt] = {}
        for dim in dimensions:
            # total view counts per dimension, if product_id, also compute mean price
            # total $$$ amount sold per dimension, if product_id also compute count and mean
            if dim == 'product_id':
                if evt == 'view':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.count('price'),F.mean('price')))
                elif evt == 'purchase':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.sum('price'),F.count('price'),F.mean('price')))
            else:
                if evt == 'view':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.count('price')))
                elif evt == 'purchase':
                    main_gb[evt][dim] = (main_df[evt].groupby(dim, 'event_time')
                                    .agg(F.sum('price')))
    return main_gb

def merge_df(df, event, dim):
    ''' The merge process deals with scenarios when there exist two entries with
        same product_id (or brand, category etc.) and same event_time, this means
        the backlog data needs to be added to the t1 table entry
    '''
    if dim == 'product_id':
        if event == 'view':
            df = df.withColumn('total_price', F.col('count(price)') * F.col('avg(price)'))
            df = df.groupby(dim, 'event_time').agg(F.sum('count(price)'), F.sum('total_price'))
            df = df.withColumnRenamed('sum(count(price))','count(price)')
            df = df.withColumn('avg(price)', F.col('sum(total_price)') / F.col('count(price)'))
            df = df.drop('sum(total_price)')
        elif event == 'purchase':
            df = df.groupby(dim, 'event_time').agg(F.sum('sum(price)'), F.sum('count(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
            df = df.withColumn('avg(price)', F.col('sum(price)') / F.col('count(price)'))
    else:
        if event == 'view':
            df = df.groupby(dim, 'event_time').agg(F.sum('count(price)'))
            df = df.withColumnRenamed('sum(count(price))', 'count(price)')
        elif event == 'purchase':
            df = df.groupby(dim, 'event_time').agg(F.sum('sum(price)'))
            df = df.withColumnRenamed('sum(sum(price))', 'sum(price)')

    return df
