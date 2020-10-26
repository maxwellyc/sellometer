## Airflow DAGs
Airflow DAGs are contained in airflow/dags directory so that airflow can find the DAG files.
Currently there are two DAGs:
1. data_transport: Manages tasks that support the 3-tiered data storage,ie minute-level, hourly, gzipped CSV. Also schedules backlog processing task.
2. streaming: Manages tasks that converts server-sent CSV files into minute-level data used for live monitoring, also controls task that maintains 24-hour window of minute-level data tables.
