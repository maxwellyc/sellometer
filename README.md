# Sellometer

Sellometer is a data pipeline that processes real-time online events of a retail website and stores the transformed data into a tiered database to support various business needs such as live monitoring and sales analytics etc.

[Demo Slides](https://docs.google.com/presentation/d/1i-34t8AvreuHTTEn04651ZIpnlO9v5nVoev7dgu0TBY/edit?usp=sharing), [Web UI](http://sellometer.xyz/), [Recorded UI Demo](https://www.youtube.com/watch?v=iGup8EMZQYc)

## Table of Contents
1. [Introduction](README.md#introduction)
2. [Architecture](README.md#architecture)
3. [Dataset](README.md#dataset)
4. [Engineering Challenges](README.md#engineering-challenges)
5. [Airflow DAGs](README.md#airflow-dags)
5. [Setup](README.md#setup)

## Introduction
Large online retailers such as Amazon and eBay receive millions of transactions per hour across billions of product listings. Other user behaviors such as viewing a product, add to cart, remove from cart are also being tracked by the web servers during a shopping trip.
Typically, these data will be processed in batch, on a day to day basis. However, there is also a pressing need for real-time monitoring of such events, a few examples would be:
#### Abnormality detection:
A wrong item listing price can cause huge loss to the retailer, and such events cannot be detected by simply monitoring the web servers' health.
#### Low inventory alert:
Having up-to-date knowledge of item inventory can prevent loss of sales due to items running out of stock, and can also be integrated into the retailer's logistical chain to drive down logistical costs on the long run.
#### Left-in-cart reminder:
Impulse buying constitutes a large portion of retail sales [source](https://www.handystorefixtures.com/blog/impulse-buying-statistics-every-retailer-should-know), items that are added and left in cart can quickly lose their appeal to a wavering consumer, having a live system that can monitor this behavior allows the retailers to send a quick email reminder to the consumer, along with up-to-date recommendation of similar products could dramatically boost sales.

## Architecture
![pipeline](https://raw.githubusercontent.com/maxwellyc/sellometer/master/images/pipeline.png)

The architecture (data pipeline) is shown above. First, an AWS EC2 instance will move CSV files from an AWS S3 bucket to another in order to (crudely) imitate the behavior of web servers sending preprocessed data files. Once the files are in the destination bucket, an S3 sensor in the Apache Airflow DAG will be triggered and its downstream Spark task will be triggered to process data in the CSV files and transforms the data into a schema optimized for live monitoring, which will be stored in a PostgreSQL database, and finally a Grafana dashboard will serve as the web UI.

## Dataset
Behavior data across 7 months (10/2019 - 4/2020) from a large multi-category online store can be obtained from [Kaggle](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Oct.csv) and [this Google Drive](https://drive.google.com/drive/folders/1Nan8X33H8xrXS5XhCKZmSpClFTCJsSpE) in CSV format.
The combined size of these CSV files is around 55 GB.
![dataset](https://raw.githubusercontent.com/maxwellyc/sellometer/master/images/dataset.png)

#### Data concentration
The original dataset listed above is very sparse in terms of behavior (events) over unit time, in order to mimic the traffic of major online retailers such as Amazon.com, these 7 months worth of data was concentrated into roughly 8 days (30:1). This concentration provided us with an average hourly sales around 10 Million USD, at the same order of magnitude as Amazon.com.

#### Data cleaning
Category_code and brand can have null values. Some products have 3 layers of categories / sub-categories, such as for the second row product in the dataset above: "appliance - environment - water_heater", others can have only two or one.

Null brand values are filled using product_id, null category_code are filled using category_id (numeric).
Partly missing categories are filled with the category directly above, eg.
    electronics.smartphone -> electronics.smartphone.smartphone

Viewing of the same product with the same user_session is only counted as one viewing event.

#### Data transformation
The concentrated data has time resolution in seconds (same as the original). These are often times too granulized to be graphed, as even the most popular items are sold in single digit quantities every second. Thus, live monitoring data is prepared on a minute-level granularity, and then further grouped into different product dimensions, ie product_id, brand, and 3 levels of category.
As an example, the schema for purchase events, grouped by product_id is:

```
|-- product_id  (int),
|-- event_time  (date),
|-- GMV         (float),
|-- count       (int),
|-- avg_price   (float)
```
GMV stands for Gross Merchandise Value which indicate a total sales monetary-value for merchandise sold for a given time period.

#### Errors in the data

A long period with completing zero purchase events is present in the data, while viewing event looks normal.

Some time during 2019 December - 2020 January , the category_code for items that were originally "electronics.smartphone" has been permanently changed into "construction.tools.light", or unless Apple and Samsung started making thousand-dollar light bulbs that I'm not aware of and all of a sudden became a huge hit (~67% of sales).

## Engineering Challenges

#### Tiered data storage
The minute-level data accumulates at a rate of ~1.3 GB / day (resulted from 8 GB of input data). If all were stored in one giant data table, the latency of querying will become larger and larger, eventually reaching a point where live monitoring losses isn't really 'live'. Given the use case of live monitoring, data on a larger time scale, such as more than 24 hours ago, is less likely to be visited. Therefore, to control the query latency and also to preserver storage, a tiered data storage scheme was devised.

The first tier data storage is designed for live monitoring, which contains minute-level data for the past 24 hours. Earlier data will be deleted from the data tables every hour, the combined size of these data tables is roughly 1.3 GB.

The second tier data storage is a set of data tables using aggregated data of the first tier, with hourly granularity, this will store all hourly data from the past month. In the current project, since only 8 days of concentrated data is available, this tier of storage will keep accumulating until the input data runs out. The hourly data accumulates at a rate of ~200 MB / day.

The third tier data storage serves the purpose of long term bookkeeping, which will compress multiple input files (mini CSV files) over an hour or a day, into one gzipped CSV file.
For scalability, one could always insert arbitrarily more tiers into this scheme, using the same code that is being used to transfer data from the first tier into the second tier, or introduce other time granularity ratio between tiers, as these are fully customizable from the design of the code.

#### Backlog processing
In the event that one or more web servers (among many) experience server hiccup or network congestion, data in the live monitoring data tables could've already moved far ahead in terms of time, which will require certain metrics to be re-aggregated and corrected in the minute-level and hourly data tables.
These files from an earlier time period that have "missed the train", will be identified and moved into a "backlogs" directory that stores backlog files on AWS S3 by the real-time processing task. An Apache Airflow sensor will monitor this directory every 10 minutes, if backlog files are present, the backlog processing task will be activated to process and correct the corresponding entries in the minute-level data tables used for live monitoring.

Unfortunately during the course of this 3 weeks Insight project, this has not been implemented to also correct the entries in the hourly data tables, but the fundamental idea is identical.

#### Event surge (dynamic resource allocation)
In case of an event surge, such as a build up of files due to the same reasons that could cause backlogs, or the retailer is experiencing a surge in network traffic depending on the time of day or due to holiday seasons, the computation resources will be reallocated to prioritized real-time processing to support live monitoring. This is implemented by detecting the combined size of real-time files waiting to be processed. Other activities such as data movement from first to second tier storage and file compression will be put on hold.

#### Race condition
Race condition could occur when various tasks mentioned above sometimes working on the same data tables, which can cause apparent issues related to the reliability of data being stored. For example, the backlog processing task would have to first read the minute-level data tables into a Spark DataFrame, eg. last data point from 10:45 pm, and then proceeds to merge the backlog data points with the data points already in place. During this time, the live processing cluster can be writing new data from 10:46 pm into the same data tables. Once the time the backlog processing task finishes its processing, it'll proceed to overwrite the minute-level data table with the set of data table ending at 10:45 pm, completely erasing the 10:46 pm data point.

Currently this is handled by forcing the backlog processing task to occupy all computation resources and prevent the real-time processing tasks from running, this corresponds to the [current Airflow DAG](README.md#airflow-dags) setup. Another solution would be to set the backlog processing task directly upstream of the real-time processing task in the DAG (see figure below).
![Backlog DAG](https://raw.githubusercontent.com/maxwellyc/sellometer/master/images/backlog_dag.png)

Both the above solutions were tried and neither is ideal since they would all disrupt / delay real-time processing. A third solution that recently came to my mind is to identify the backlog files' timestamps, and directly perform INSERT and DELETE on the SQL level once the correct data is computed and merged with what's in the original data table, this solution does not involve the overwriting action and thus does not impact entries of another timestamp.


## Airflow DAGs
The current DAGs for real-time processing (top) and housekeeping (bottom):
![Airflow DAGs](https://raw.githubusercontent.com/maxwellyc/sellometer/master/images/airflow.png)

## Setup
#### Tools Version
Database: PostgreSQL 10.14 (jdbc driver jar: postgresql-42.2.16.jre7.jar)

Task scheduler: Apache Airflow 1.10.12

Computing framework: Apache Spark 2.4.3 Using Scala version 2.11.12, OpenJDK 64-Bit Server VM, 1.8.0_265

Spark runtime packages: --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7

Web UI: Grafana 7.2.1

#### AWS resources
Spark Cluster: 8 m5a.large (2 vCPUs, 8GB, Ubuntu 18.04.5 LTS) EC2 instances

PostgreSQL: 1 m5a.large (2 vCPUs, 8GB, Ubuntu 18.04.5 LTS) EC2 instance

Web hosting:  1 m5a.large (2 vCPUs, 8GB, Ubuntu 18.04.5 LTS) EC2 instance

Imitate web server: 1 m5a.large (2 vCPUs, 8GB, Ubuntu 18.04.5 LTS) EC2 instance
