## Table of Contents
1. [Main data processing](README.md#main-data-processing)
2. [Reusable functions](README.md#reuseable-functions)
3. [Config](README.md#config.py)

## Main data processing

#### ingestion.py
Module for processing (real-time) CSV files containing online events and store into minute-level data table (t1 table) in PostgreSQL DB.
Also identifies backlog CSV files (files with event time earlier than that already in the t1 table), and partitions CSV files into appropriate folders, either <backlogs> or <processingpool> on AWS S3.

#### data_transport.py
Read data from minute-level data table (PostgreSQL) and aggregates into hourly data, finally storing the hourly data table (t2 table) into PostgreSQL DB.
Also creates table for ranking purpose (sliding window).

#### daily_window.py
Maintains 24-hour window for minute-level PostgreSQL data table. This will soon be replaced by direct SQL commands which will dramatically speed up the process. (10/23/2020)

#### backlog_processing.py
Process backlog CSV files located inside <backlogs> folder on AWS S3, the data processing (from CSV) is identical to the main ingestion process, and in fact uses ingestion.py
for said processing. This module then combines the backlog (Spark) data with data from the main t1 table, re-aggregate rows that now have duplicates (event_time, product_id/brand/category) due to backlog.
Currently this does not correct the t2 table, but the idea is identical to fixing the t1 table.

#### log_compression.py
Compresses several small CSV files into a larger, gzipped CSV file. The CSV files sent from the servers are minute-by-minute, these data are read into Spark dataframe together and then written back into a gzipped CSV file across an hour or a day, depending on customizable variable.

## Reusable functions
#### utility.py
Contains utility functions that deals with time(stamps), file handling with AWS S3 and IO operation with PostgreSQL database.

#### processing_funcs.py
This module contains reusable functions that process and transform Spark dataframes, these functions are used frequently in ingestion.py, data_transport.py,
and backlog_processing.py

## config.py
Currently contains 3 global variables used in the main data processing modules. Other variables that indicates S3 path locations etc should also be integrated here in the future.
