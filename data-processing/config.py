# Stores input parameters for the pipeline

# datatables will be named based on dimension and event: {event}_{dimension}_{suffix}
# valid {suffix} can be 'minute', 'hour', 'rank', there could be additional suffixes
# for temporay storage purpose but not for actual data storage.

# product dimensions: product_id for individual product, brand, category_l1 to _l3
# list variable used in ingestion.py, backlog_processing.py, data_transport.py
# and daily_window.py
dimensions = ['product_id', 'brand', 'category_l3']#, 'category_l2', 'category_l3']

# online event type, can be 'purchase', 'view', 'cart'
# list variable used in ingestion.py, backlog_processing.py, data_transport.py
# and daily_window.py
events = ['purchase', 'view']

# Can be 'hour' or 'day' for compressing files into block size of one hour or one day,
# string variable used in logs_compression.py
compress_block = 'hour'
