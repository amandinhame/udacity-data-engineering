# Data Warehouse on AWS

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The objective of this project is to build an ETL pipeline for a database hosted on Amazon Redshift with data from AWS S3.

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


## Requirements

* Python 3.6
* Python packages:
	* psycopg2
	* boto3

## Project files
|File|Description|
|---|---|
|create_cluster.py|Creates redshift cluster|
|create_tables.py|Drops and creates tables (reset schema)|
|delete_cluster.py|Deletes redshift cluster|
|dwh.cfg|Configuration file|
|etl.py|Process dataset files and load the tables|
|sql_queries.py|Contains the queries used in the project|

## Running

1. Insert the KEY and SECRET in the dwh.cfg file with information of a valid credential of a AWS user.
2. Create a cluster in Amazon Redshift

```
python create_cluster.py
```

3. Create the tables in the Amazon Redshift database:

```
python create_tables.py
```

4. Run the ETL pipeline to process the dataset files and fill the tables:

```
python etl.py
```

It will also show you the number of rows in each table at the end of the process.

5. At the end of the analysis delete the cluster to avoid expenditures.

```
python delete_cluster.py
```

## Examples of queries for analysis

### Number of users using the app in a specific month
```
SELECT 
	COUNT(DISTINCT user_id) 
FROM songplays 
JOIN time 
ON 
	songplays.start_time = time.start_time 
WHERE 
	year = 2018 AND 
	month = 11
```
|count|
|---|
|55|

### Top 5 artists of all time

```
SELECT
	artists.name, 
	count(songplays.artist_id) AS times_played
FROM songplays 
JOIN artists 
ON 
	songplays.artist_id = artists.artist_id 
GROUP BY 
	artists.name 
ORDER BY times_played DESC
LIMIT 5
```

|name|times_played|
|---|---|
|Dwight Yoakam	|37|
|Kid Cudi / Kanye West / Common	|10|
|Kid Cudi	|10|
|Ron Carter	|9|
|Lonnie Gordon	|9|