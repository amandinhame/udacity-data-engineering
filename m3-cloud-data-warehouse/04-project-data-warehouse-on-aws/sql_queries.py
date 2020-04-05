import configparser
from create_cluster import get_iam_role_info


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR(256),
    auth            VARCHAR(16),
    firstName       VARCHAR(256),
    gender          CHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR(256),
    length          FLOAT,
    level           VARCHAR(16),
    location        VARCHAR(256),
    method          VARCHAR(16),
    page            VARCHAR(128),
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR(256),
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR(256),
    userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR(18),
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR(256),
    artist_name         VARCHAR(256),
    song_id             VARCHAR(18),
    title               VARCHAR(256),
    duration            FLOAT,
    year                INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INTEGER IDENTITY(0, 1) PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL,
    user_id         INTEGER NOT NULL,
    level           VARCHAR(16),
    song_id         VARCHAR(18) NOT NULL,
    artist_id       VARCHAR(18) NOT NULL,
    session_id      INTEGER,
    location        VARCHAR(256),
    user_agent      VARCHAR(256)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER NOT NULL PRIMARY KEY,
    first_name  VARCHAR(256),
    last_name   VARCHAR(256),
    gender      VARCHAR(1),
    level       VARCHAR(16)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR(18) NOT NULL PRIMARY KEY,
    title       VARCHAR(256),
    artist_id   VARCHAR(18) NOT NULL,
    year        INTEGER,
    duration    FLOAT
); 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR(18) NOT NULL PRIMARY KEY,
    name        VARCHAR(256),
    location    VARCHAR(256),
    latitude    FLOAT,
    longitude   FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP NOT NULL PRIMARY KEY,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER,
    weekday     INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    credentials 'aws_iam_role={}'
    region '{}'
    format as json {}
    timeformat as 'epochmillisecs'
""").format(
    config.get('S3', 'LOG_DATA'),
    get_iam_role_info(config),
    config.get('AWS', 'REGION'),
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs 
    from {}
    credentials 'aws_iam_role={}'
    region '{}'
    format as json 'auto'
""").format(
    config.get('S3', 'SONG_DATA'),
    get_iam_role_info(config),
    config.get('AWS', 'REGION'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    se.ts AS start_time,
    se.userId AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionId AS session_id,
    se.location AS location,
    se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss
ON 
    se.song = ss.title AND
    se.artist = ss.artist_name AND
    se.length = ss.duration
WHERE
    se.page = 'NextSong' AND
    se.sessionId NOT IN (
        SELECT s.session_id
        FROM songplays s
        WHERE
            s.start_time = se.ts AND
            s.user_id = se.userId AND
            s.session_id = se.sessionId
    )
;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT(se.userId) AS user_id,
    se.firstName AS first_name,
    se.lastName AS last_name,
    se.gender AS gender,
    se.level AS level
FROM staging_events se
WHERE
    se.page = 'NextSong' AND 
    se.userId IS NOT NULL AND
    se.userId NOT IN (
        SELECT u.user_id
        FROM users u
        WHERE u.user_id = se.userId
    )
;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT(ss.song_id) AS song_id,
    ss.title AS title,
    ss.artist_id AS artist_id,
    ss.year AS year,
    ss.duration AS duration
FROM staging_songs ss
WHERE 
    ss.song_id IS NOT NULL AND
    ss.song_id NOT IN (
        SELECT s.song_id
        FROM songs s
        WHERE s.song_id = ss.song_id
    )
;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT(ss.artist_id) AS artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS latitude,
    ss.artist_longitude AS longitude
FROM staging_songs ss
WHERE 
    ss.artist_id IS NOT NULL AND
    ss.artist_id NOT IN (
        SELECT a.artist_id
        FROM artists a
        WHERE a.artist_id = ss.artist_id
    )
;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT 
    DISTINCT(s.start_time) AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dayofweek FROM start_time) AS weekday
FROM songplays s
WHERE s.start_time NOT IN (
    SELECT t.start_time
    FROM time t
    WHERE t.start_time = s.start_time
)
;
""")

# COUNT TABLES

staging_events_table_count = "SELECT COUNT(*) FROM staging_events;"
staging_songs_table_count = "SELECT COUNT(*) FROM staging_songs;"
songplay_table_count = "SELECT COUNT(*) FROM songplays;"
user_table_count = "SELECT COUNT(*) FROM users;"
song_table_count = "SELECT COUNT(*) FROM songs;"
artist_table_count = "SELECT COUNT(*) FROM artists;"
time_table_count = "SELECT COUNT(*) FROM time;"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_table_queries = [staging_events_table_count, staging_songs_table_count, songplay_table_count, user_table_count, song_table_count, artist_table_count, time_table_count]