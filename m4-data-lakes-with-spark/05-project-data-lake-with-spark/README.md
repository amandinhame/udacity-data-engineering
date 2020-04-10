# Data Lake with Spark

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The objective of this project is to build an ETL pipeline that extracts data from S3, processes them using Spark and loads the data back to S3 as a set of dimensional tables.

## Datasets

The datasets are hosted in S3 in the paths:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

### Song dataset

The song dataset is a collection of JSON files from [Million Song Dataset](http://millionsongdataset.com/) and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset:

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And an example of the contents of one of the files:

```
{
    "num_songs": 1, 
    "artist_id": "ARMJAGH1187FB546F3", 
    "artist_latitude": 35.14968, 
    "artist_longitude": -90.04892, 
    "artist_location": "Memphis, TN", 
    "artist_name": "The Box Tops", 
    "song_id": "SOCIWDW12A8C13D406", 
    "title": "Soul Deep", 
    "duration": 148.03546, 
    "year": 1969
}
```

### Log dataset

The log dataset are JSON files created with [eventsim](https://github.com/Interana/eventsim/) which simulates activity logs from a music stream app. They are partitioned by year and month of the event:

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And an example of one of the rows in one of the files:

```
{
	"artist":null,
	"auth":"Logged In",
	"firstName":"Ryan",
	"gender":"M",
	"itemInSession":0,
	"lastName":"Smith",
	"length":null,
	"level":"free",
	"location":"San Jose-Sunnyvale-Santa Clara, CA",
	"method":"GET",
	"page":"Home",
	"registration":1541016707796.0,
	"sessionId":169,
	"song":null,
	"status":200,
	"ts":1541109015796,
	"userAgent":"\"Mozilla\/5.0 (X11; Linux x86_64) AppleWebKit\/537.36 (KHTML, like Gecko) Ubuntu Chromium\/36.0.1985.125 Chrome\/36.0.1985.125 Safari\/537.36\"",
	"userId":"26"
}
```

## Schema

### Fact Table

#### songplays

|Field name|
|---|
|start_time      |
|user_id         |
|level           |
|song_id         |
|artist_id       |
|session_id      |
|location        |
|user_agent      |
|year|
|month|

### Dimension Tables

#### users

|Field name|
|---|
|user_id|
|first_name|
|last_name|
|gender|
|level|

#### songs

|Field name|
|---|
|song_id|
|title|
|artist_id|
|year|
|duration|

#### artists

|Field name|
|---|
|artist_id|
|name|
|location|
|latitude|
|longitude|

#### time

|Field name|
|---|
|start_time|
|hour|
|day|
|week|
|month|
|year|
|weekday|


## Requirements

* Python 3.6
* PySpark 2.4
* Python packages:
	* configparser
	* pyspark
* Aws account with:
    * Access key/Secret of a user with S3 permissions
    * Key pair to access cluster
    * S3 bucket to save final tables/data
    * Access to AWS EMR

The access to AWS EMR is necessary only if you want to run the etl in a cluster. You can execute the etl script on you own machine with a smaller dataset by running: 

```
python etl.py
```

## Project files

|File|Description|
|---|---|
|dl.cfg|Configuration file|
|etl.py|Process dataset files from S3 and save the results back in S3|

## Running

1. Insert the KEY and SECRET in the dl.cfg file with information of a valid credential of an AWS user with S3 permission.

2. In the etl.py file update the 'output_data' variable with a S3 path where the final data will be stored.

3. Copy the updated project files to a S3 bucket.

4. Create a cluster in AWS EMR specifying a key pair to access the cluster master node. I've used the following configuration:

* Regions us-west-2 (same of S3 datasets)
* Release label: emr-5.20
* Spark 2.4.0
* Instance Type: m5.xlarge
* Number of instances: 3 (+ 2 spot task nodes)

5. After the cluster has started, log in the master node using the instructions in the 'SSH' link located in the Summary tab.

6. In the terminal, logged in the master node, copy the project files from S3 to the cluster:

```
aws s3 cp s3://[your-s3-project-bucket]/etl.py /home/hadoop/
aws s3 cp s3://[your-s3-project-bucket]/dl.cfg /home/hadoop/
```

7. In my case I also had to update pip and configparser:

```
sudo pip install --upgrade pip
/usr/local/bin/pip install configparser
```

8. Run the spark job:

```
spark-submit --master yarn ./etl.py
```

9. Check if your S3 bucket has now the data in parquet format.

10. After the job is finished terminate the AWS EMR cluster to avoid expenditures.