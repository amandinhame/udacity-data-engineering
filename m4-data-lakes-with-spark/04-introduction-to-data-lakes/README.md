# Introduction to Data Lakes

## Evolution of Data Warehouse

* Unprecedented data volumes
* Unstructured data (json, xml, logs, etc)
* Big data technologies (Hadoop, Spark, etc)
* New types of data analysis
* New roles involving data

## Data Lake

Is a new form of data warehouse that evolved to cope with:
* Varying of data format and structures.
* Agile and ad-hoc nature of data exploration activities needed by new roles like the data scientist.
* The wide expectrum data transformation needed by advanced analytics like machine learning, graph analytics, and recomender systems.

## Big Data Effects on Data Warehouse

* ETL Offloading (one big cluster for storage and processing)
* Schema-on-read (no need to create a database and insert the data into in. It's possible to read data from files with a specified or inferred schema).
* (Un/Semi)Structured support (many types of files, variety of file systems, variety of databases)

## AWS Data Lake Options

* AWS EMR (HDFS + Spark)
* AWS EMR (S3 + Spark)
* AWS Athena