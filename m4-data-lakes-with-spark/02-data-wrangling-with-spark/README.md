# Data Wrangling with Spark

## Functional Programming

Functional programming is ideal for distributed systems, since it always returns the same output given an input.

## Spark DAGs

Spark makes copies of the original data and never changes it -> Immutible

Spark uses lazy evaluation, making the receipe the data will follow -> Directed Acyclical Graph.

## Maps and Lambda Functions

Since spark uses lazy evaluation, it waits until the last possible moment to transform the data. To force, we can use the collect function.

```
distributed_song_log.map(lambda x: x.lower()).collect()
```

## Distributed Data Stores

HDFS splits files into 64 or 128 MB blocks and distributes it across the cluster in a fault tolerant way

## RDDs

The query/code in spark goes through a query optimizer called Catalysts, which trnasforms the execution plan into a DAG.

RDD: Resilient Distributed Dataset. They are the low level abstraction of the data in spark.