To see how backlog processing work:
1. Manually move files into s3://maxwell-insight/serverpool/ to generate initial sql table, these files should be of a later date, so you'll have earlier data files that are classified as backlogs
2. Manually run homekeep_check_backlog.py to move backlog files into backlog folder on s3, inspect if these is correct, based on datetime stored in logs/last_tick.txt
3. Before running homekeep_process_backlog.py using spark-submit, record datatables, so you can compare with after.
4. spark-submit homekeep_process_backlog.py, check datatables after processing, this should have the correct aggregated results.
