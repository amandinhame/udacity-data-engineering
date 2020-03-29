
# Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The objective of this project is to create and model a database with Postgres to optimize queries for the analysis and to build an ETL pipeline using python to transfer the data from the files to the database.

## Datasets

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

To optimize the queries and analysis of the song play a optimized star schema as following was built.

### Fact table

#### songplays
|Field name|Type|Constraint|
|---|---|---|
|songplay_id|SERIAL|PRIMARY KEY|
|start_time|TIMESTAMP|NOT NULL|
|user_id|INT|NOT NULL|
|level|VARCHAR(16)||
|song_id|VARCHAR(18)||
|artist_id|VARCHAR(18)||
|session_id|INT||
|location|VARCHAR(128)||
|user_agent|VARCHAR(256)||

### Dimension tables

#### users

|Field name|Type|Constraint|
|---|---|---|
|user_id| INT| PRIMARY KEY|
|first_name| VARCHAR(64)||
|last_name| VARCHAR(64)||
|gender| VARCHAR(1)||
|level| VARCHAR(16)||

#### songs

|Field name|Type|Constraint|
|---|---|---|
|song_id| VARCHAR(18)| PRIMARY KEY|
|title| VARCHAR(128)||
|artist_id| VARCHAR(18)| NOT NULL|
|year| INT||
|duration| NUMERIC||

#### artists

|Field name|Type|Constraint|
|---|---|---|
|artist_id| VARCHAR(18)| PRIMARY KEY|
|name| VARCHAR(128)||
|location| VARCHAR(128)||
|latitude| NUMERIC||
|longitude| NUMERIC||

#### time

|Field name|Type|Constraint|
|---|---|---|
|start_time| TIMESTAMP| PRIMARY KEY|
|hour| INT||
|day| INT||
|week| INT||
|month| INT||
|year| INT||
|weekday| INT||


## Requirements

* Python 3.6
* Postgres 9.5
* Python packages:
	* psycopg2 2.7
	* pandas 0.23

## Project files
|File|Description|
|---|---|
|create_tables.py|Drops and creates tables (reset schema)|
|etl.ipynb|Process one file of song and log data as example|
|etl.py|Reads and process dataset files and load the tables|
|sql_queries.py|Contains the queries used in the project|
|test.ipynb|Checks the data in the tables|

## Running

1. Configure a Postgres database on your local machine with the following information:

```
dbname=studentdb
user=student 
password=student
```

Or change the database configuration in the following files:
* create_tables.py
* etl.py
* etl.ipynb
* test.ipynb

2. Download the songs and logs datasets to their respective directory structures.
3. Create the schema in the Postgres database with the command line:

```
python create_tables.py
```
4. Run the ETL pipeline to read and process the dataset files to fill the tables:

```
python etl.py
```
5. To test and check the database you can run the test.ipynb notebook.

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
|96|

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
ORDER BY times_played 
LIMIT 5
```
Since I used only a small subset of the dataset it only returned one register.

|name|times_played|
|---|---|
|Elena|1|