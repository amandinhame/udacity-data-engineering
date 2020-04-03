# Implementing Data Warehouses on AWS

## Choices for implementing a DWH

* On-premise: No heterogeinity, scalability and elasticity, need of IT staff, and high cost of ownership
* Cloud: Lower barrier entry, may add as you need, scalability, elasticity
    * Cloud-managed: RDS, DynamoDB, S3
    * Self-managed: EC2 + Postgres, EC2 + Cassandra, EC2 + Unix FS

## Aws Redshift technology

* Column-oriented storage
* Best for OLAP workloads
* Modified Postgres
* MPP (MAssive Parallel Processing): parallelize the execution of one query on multiples CPUs
* Partitioned tables are processed in parallel

## Redshift archtecture

* 1 Leader node: 
    * Coordinates compute nodes
    * Handles external communications
    * Optimizes query execution
* N Compute nodes:
    * Each own CPU, memory and disk
    * Scale up: more powerful nodes
    * Scale out: more nodes
    * Divided into node slices. A cluster with n slices can process n partitions of tables simultaneously.

## SQL to SQL ETL
    DB1 -> select -> ETL Server -> insert/copy -> DB2

    RDS -> EC2 / S3 -> Redshift

## Ingest at scale
* To transfer (csv) data from S3 staging area to Redshift use COPY command
* INSERT command is slow
* If file is large
    * Break in multiple files
    * Ingest in parallel (using common prefix or manifest file)
* Ingest from same aws region
* Compress csv files

## Infrastructure as Code in AWS
* aws-cli scripts
* AWS SDK
* Amazon Coud Formation

## Optimizing Table Design

* Distribution style
* Sorting key

## Distribution Style: Even

* Balance the number of rows in each slice. 
* Good if table won't be joined, otherwise it will result in a lot of shuffling.

## Distribution Style: All

* Tables are replicated in all slices (broadcasting).
* Distributing fact tables (usually bigger) with 'even' and dimension (smaller) with 'all' eliminates shuffling.

## Distribution Style: Auto

* Leave decision to Redshift
* Small tables with 'all' strategy
* Large tables with 'even' strategy

## Distribution Style: Key

* Rows having similar values are placed in the same slice
* Eliminates shuffling
* Can lead to skewed distribution

## Sorting Key



