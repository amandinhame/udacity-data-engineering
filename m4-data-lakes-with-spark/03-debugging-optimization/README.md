# Debugging and Optimization

## Spark Cluster Manager

* Standalone Mode
* MESOS
* YARN

## AWS EMR Setup Instructions 

* EMR 5.20.0
* Spark 2.4
* m5.xlarge
* Yarn mode
* 4 Instances (1 master 3 core nodes)

## EC2 Machine Types

* M: Multipurpose
* R: RAM optimized
* C: Compute optimized

## Files in Spark

Spark can read from S3 or HDFS.

E.g.:

* S3: s3n://sparkify/file.json
* HDFS: hdfs:///user/sparkify/file.json

## HDFS

Some commands to write to HDFS in the cluster

* hdfs dfs -mkdir <path>
* hdfs dfs -copyFromLocal <from> <to>

## Debuggin with accumulators

```
from pyspark.sql.functions import udf

incorrect_records = SparkContext.accumulator(0, 0)

def add_incorrect_record():
    global incorrect_records
    incorrect_records += 1

correct_ts = udf(lambda x: 1 if x.isdigit() else add_incorrect_record())

logs = logs.where(logs['_corrupt_record'].isNull()).withColumn('ts_digit', correct_ts(logs.ts))

logs.collect()

incorrect_records.value
```

Each time collect is called in this case the number of incorrect_records increased. So we have to use it carefully.

## Spark UI

* Ports 4040 or 8080
* A stage is a unit of work that depend on one another and can be parallelized.
* A task is the smallest unit in a stage. They are a series of transformations that can run in parallel.
* n tasks completes a stage.

## Spark Problems

### Data Skew

If the data is not well divided it can lead to a data skew, which slows down the entire process.
To detect this problem we can run the spark job with a small part of the data.

Solutions:
* Change the workload division (alternative key)
* Partition the data (adjust spark.sql.shuffle.partitions parameter)

### Bad Algorithm

Bad algorithms can make the spark job too slow.

### Insuficient Resources

If possible increase the number of executors or use more powerful machines.

It may be useful to understand:
* How mush data you are processing (compressed files can be tricky to interpret).
* If it is possible to decrease the amount of data is filtered or aggregated.
* The resource utilization.