# Lesson 4 - NoSQL Data Models

## Terminology

NoSQL = Not Only SQL -> Non Relational Databases

## When to use NoSQL

* Need high availability
* Large amount of data
* Need linear scalability
* Low latency
* Need fast read/write

## Distributed databases

* Copies of data are needed in order to have a high availability
* Eventual consistency: data may be different in different locations for a short period of time
* In Apache Cassandra every node is connected to every node

## CAP Theorem

The CAP theorem says a data store can only guarantee two of the Consistency, Availability and Partition Tolerance.

* Consistency: Always get the last correct data (or an error)
* Availability: Always receive a response
* Partition Tolerance: System continues to work regardless of losing connectivity between nodes

Apache Cassandra guarantees Availability and Partition Tolerance.

## Data Modeling In Apache Cassandra

* You NEED to denormalize your tables
* It is optimized for fast writes
* Need to think of your queries first
* No joins between tables
* One table for query is a good strategy

## CQL - Cassandra Query Language

* Similar to SQL
* There are no Joins, Group By and subqueries in CQL
* Keyspace similar to schema

## Primary Key

* Can be just the Partition Key (Simple) or Partition Key + Clustering Key (Composite)
* Must be unique (an error is not thrown when overwriting the value)
* Hashing of this value results in placement on a particular node in the system
* Cassandra does not allow duplicated rows

## Clustering Columns

Primary key is made up of just the partition key or with the addition of clustering columns. The clustering columns will determine the sort order within a partition.

* Will sort the data in a ascendent order
* Can be more than one column
* The columns will be sorted in the order they were added to the primary key

In the following table 'year' is the partition key and 'artist_name' and 'album_name' are clustering columns.

```
CREATE TABLE music_library (
    year int,
    artist_name text,
    album_name text,
    PRIMARY KEY ((year), artist_name, album_name)
)
```

## Where Clause

* It's not recommended that we do a select without a WHERE clause (needs configuration)
* The order of fields in the where clause must follow the order in the primary key