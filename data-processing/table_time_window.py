''' Maintains time window of tables in PostgreSQL DB using SQL calls.
'''
import datetime as dt
import imp
import os
import psycopg2
# load self defined modules
UTIL = imp.load_source('UTIL', '/home/ubuntu/eCommerce/data-processing/utility.py')
CONFIG = imp.load_source('CONFIG', '/home/ubuntu/eCommerce/data-processing/config.py')

def sql_query(events, dimensions, query):
    ''' Make SQL calls directly in SQL database (PostgreSQL) '''
    try:
        connection = psycopg2.connect(user=os.environ['psql_username'],
                                      password=os.environ['psql_pw',
                                      host="10.0.0.5",
                                      port="5431",
                                      database="ecommerce")

        cursor = connection.cursor()
        cursor.execute(";".join(query), multi=True)
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        pass

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()

def maintain_time_window(events, dimensions, suffix='minute', window_hours=24):
    ''' Maintain data tables to a fixed size, depending on the size of the time window.
        For t1 tables, use suffix='minute'; for t2 tables, suffix='hour'.
    '''
    sql_c, spark = spark_init('maintain_time_window')
    max_time = UTIL.get_latest_time_from_db(spark, suffix=suffix)
    cutoff = UTIL.datetime_to_str(max_time - dt.timedelta(hours=window_hours),
                                  '%Y-%m-%d %H:%M:%S')
    sql_delete_query = []
    for evt in events:
        for dim in dimensions:
            sql_delete_query.append(f"""DELETE FROM {evt}_{dim}_{suffix}
                                    WHERE event_time < \'{cutoff}\'
                                    """)
    sql_delete_query.append("""VACUUM FULL""")
    sql_delete(evt, dim, sql_delete_query)

    spark.stop()
    return

if __name__ == "__main__":
    maintain_time_window(CONFIG.EVENTS, CONFIG.DIMENSIONS)
