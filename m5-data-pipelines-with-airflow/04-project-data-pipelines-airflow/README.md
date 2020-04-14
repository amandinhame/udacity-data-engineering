# Data Pipelines With Airflow

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The objective of this project is to build an ETL pipeline using Airflow for a database hosted on Amazon Redshift with data from AWS S3.

## Datasets

The datasets are hosted in S3 in the paths:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data
* Log data json path: s3://udacity-dend/log_json_path.json

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

### Staging tables

#### staging_events

|Field name|Type|Constraint|
|---|---|---|
|artist          |VARCHAR(256)||
|auth            |VARCHAR(16)||
|firstName       |VARCHAR(256)||
|gender          |CHAR(1)||
|itemInSession   |INTEGER||
|lastName        |VARCHAR(256)||
|length          |FLOAT||
|level           |VARCHAR(16)||
|location        |VARCHAR(256)||
|method          |VARCHAR(16)||
|page            |VARCHAR(128)||
|registration    |FLOAT||
|sessionId       |INTEGER||
|song            |VARCHAR(256)||
|status          |INTEGER||
|ts              |TIMESTAMP||
|userAgent       |VARCHAR(256)||
|userId          |INTEGER||

#### staging_songs

|Field name|Type|Constraint|
|---|---|---|
|num_songs           |INTEGER||
|artist_id           |VARCHAR(18)||
|artist_latitude     |FLOAT||
|artist_longitude    |FLOAT||
|artist_location     |VARCHAR(256)||
|artist_name         |VARCHAR(256)||
|song_id             |VARCHAR(18)||
|title               |VARCHAR(256)||
|duration            |FLOAT||
|year                |INTEGER||

### Analytic tables

#### songplays

|Field name|Type|Constraint|
|---|---|---|
|songplay_id     |INTEGER| IDENTITY(0, 1) PRIMARY KEY|
|start_time      |TIMESTAMP|NOT NULL|
|user_id         |INTEGER|NOT NULL|
|level           |VARCHAR(16)||
|song_id         |VARCHAR(18)|NOT NULL|
|artist_id       |VARCHAR(18)|NOT NULL|
|session_id      |INTEGER||
|location        |VARCHAR(256)||
|user_agent      |VARCHAR(256)||

#### users

|Field name|Type|Constraint|
|---|---|---|
|user_id| INTEGER|NOT NULL PRIMARY KEY|
|first_name| VARCHAR(256)||
|last_name| VARCHAR(256)||
|gender| VARCHAR(1)||
|level| VARCHAR(16)||

#### songs

|Field name|Type|Constraint|
|---|---|---|
|song_id| VARCHAR(18)|NOT NULL PRIMARY KEY|
|title| VARCHAR(256)||
|artist_id| VARCHAR(18)| NOT NULL|
|year| INTEGER||
|duration| FLOAT||

#### artists

|Field name|Type|Constraint|
|---|---|---|
|artist_id| VARCHAR(18)|NOT NULL PRIMARY KEY|
|name| VARCHAR(256)||
|location| VARCHAR(256)||
|latitude| FLOAT||
|longitude| FLOAT||

#### time

|Field name|Type|Constraint|
|---|---|---|
|start_time| TIMESTAMP|NOT NULL PRIMARY KEY|
|hour| INTEGER||
|day| INTEGER||
|week| INTEGER||
|month| INTEGER||
|year| INTEGER||
|weekday| INTEGER||

## Pipeline

The order of the tasks should be as in the following diagram.

![](https://video.udacity-data.com/topher/2019/January/5c48a861_example-dag/example-dag.png)

## Requirements

* Airflow
* Python 3.6
* AWS IAM user with permissions to Redshift

## Running

1. Create a cluster in Amazon Redshift.
2. In the query editor, connect to the cluster and run the script to create the tables `create_tables.sql`.
3. Allow ingress access in port 5439 to that cluster.
4. Copy the project files to the Airflow installation directory.
5. Start Airflow:
```
/opt/airflow/start.sh
```
4. In the Airflow dashboard create a Connection with the configurations:
	* Conn Id: `aws_credentials`
	* Conn Type: `Amazon Web Services`
	* Login: Access key from the IAM user
	* Password: Secret key from the IAM user
5. Create another Connection with the configurations:
	* Conn Id: `redshift`
	* Conn Type: `Postgres`
	* Host: The endpoint of the cluster (without port)
	* Schema: `dev`
	* Login: Same login of the cluster configuration
	* Password: Same password of the cluster configuration
	* Port: `5439`
6. You may wish to change the schedule configuration in the `sparkify_dag.py`.
7. Turn on the `sparkify_dag` and see if it runs successfully.
8. Don't forget to delete the Redshift cluster at the end to avoid costs.