# The Power of Spark

## Numbers everyone should know
* CPU (Fastest)[200x faster than RAM]
* Memory (Efficient, expensive, ephemeral)[15x faster than SSD]
* Storage (Slow)[20x faster than network]
* Network (Common bottleneck)

## Big Data
Usually when you can deal with the data in your local machine. One way to avoid loading all data into memory is to break it into smaller [https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-chunking](chunks).

## Distibuted Computing

Distributed computing: multiple CPUs each with its own memory.
Parallel computing: uses multiple CPUs sharing the same memory.

## Hadoop Framework

* HDFS - data storage
* MapReduce - data processing
* Yarn - resource manager
* Hadoop commons - utilities

## Newer Distributed Data Technologies

* Apache Spark - Can work on top of Hadoop and it's usually faster, since it tries to keep intermediate results in memory instead of in disk.
* Apache Storm - Streaming Data
* Apache Flink - Streaming Data

## MapReduce

* Divide large dataset and distribute across a cluster.
* Each data is analyzed and converted into (key, value) pair.
* (key, value) pairs are shuffled across the cluster so all equal keys are in the same machine.
* Values of the same keys are combined.

