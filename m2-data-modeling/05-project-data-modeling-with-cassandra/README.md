# Data Modeling with Cassandra

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

The objective of this project is to create and model a database optimized to run queries in Apache Cassandra and to build an ETL in python to transfer data from the csv files to the database.

## Dataset

We will use the event_data dataset which contains data of the events registered by the Sparkify music app. The files are partitioned by date as the following example:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

And an exemple of its contents:

|artist|auth|firstName|gender|itemInSession|lastName|length|level|location|method|page|registration|sessionId|song|status|ts|userId|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|Mr Oizo|Logged In|Kaylee|F|3|Summers|144.03873|free|Phoenix-Mesa-Scottsdale, AZ|PUT|NextSong|1.54034E+12|139|Flat 55|200|1.54111E+12|8|
|Tamba Trio|Logged In|Kaylee|F|4|Summers|177.18812|free|Phoenix-Mesa-Scottsdale, AZ|PUT|NextSong|1.54034E+12|139|Quem Quiser Encontrar O Amor|200|1.54111E+12|8|

## Query and Tables

To each query we want to run in Apache Cassandra we created a specific table.

### music_by_session

Query: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

|Field name|Type|
|---|---|
|session_id|INT|
|session_item|INT|
|artist_name|TEXT|
|song_title|TEXT|
|song_length|FLOAT|
PRIMARY KEY (session_id, session_item)

We can run this query in the music_by_session table:

```
SELECT
    artist_name,
    song_title,
    song_length
FROM
    music_by_session
WHERE
    session_id = 338 AND
    session_item = 4
```

And obtain the result:

|artist_name|song_title|song_length|
|---|---|---|
|Faithless|Music Matters (Mark Knight Dub)|495.30731201171875|

### music_user_by_user_session

Query: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

|Field name|Type|
|---|---|
|user_id|INT|
|session_id|INT|
|session_item|INT|
|artist_name|TEXT|
|song_title|TEXT|
|user_first_name|TEXT|
|user_last_name|TEXT|
 PRIMARY KEY (user_id, session_id, session_item)
 
 We can run this query on the music_user_by_user_session table:
 
 ```
 SELECT
    artist_name,
    song_title,
    user_first_name,
    user_last_name
FROM
    music_user_by_user_session
WHERE
    user_id = 10 AND
    session_id = 182
 ```
 
And obtain the result:
 
|artist_name|song_title|user_first_name|user_last_name|
|---|---|---|---|
|Down To The Bone|Keep On Keepin' On|Sylvie |Cruz|
|Three Drives|Greece 2000|Sylvie|Cruz|
|Sebastien Tellier|Kilometer|Sylvie|Cruz|
|Lonnie Gordon|Catch You Baby (Steve Pitron & Max Sanna Radio Edit)|Sylvie|Cruz|

### user_by_song

Query: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

|Field name|Type|
|---|---|
|user_id|INT|
|song_title|TEXT|
|user_first_name|TEXT|
|user_last_name|TEXT|
PRIMARY KEY (song_title, user_id)

We can run this query on the user_by_song table:

```
SELECT
    user_first_name,
    user_last_name
FROM
    user_by_song
WHERE
    song_title = 'All Hands Against His Own'
```

And obtain the result:

|user_first_name|user_last_name|
|---|---|
|Jacqueline|Lynch|
|Tegan|Levine|
|Sara|Johnson|

## Requirements
* Python 3.6
* Cassandra 3.11.6
* Jupyter notebook
* Python packages:
    * pandas 0.23
    * cassandra-driver 3.11
    * numpy 1.12
    
## Running

Follow the steps in the Jupyter notebook of this project.