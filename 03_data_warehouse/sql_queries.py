import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop  = "DROP table IF EXISTS staging_songs"
songplay_table_drop       = "DROP table IF EXISTS songplays"
user_table_drop           = "DROP table IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"
temp_songplay_table_drop  = "DROP table IF EXISTS temp_songplays"
temp_user_table_drop      = "DROP table IF EXISTS temp_users"
temp_song_table_drop      = "DROP TABLE IF EXISTS temp_songs"
temp_artist_table_drop    = "DROP TABLE IF EXISTS temp_artists"
temp_time_table_drop      = "DROP TABLE IF EXISTS temp_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth             VARCHAR,
    first_name       VARCHAR,
    gender           VARCHAR,
    item_in_sesssion VARCHAR,
    last_name        VARCHAR,
    length           VARCHAR,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     VARCHAR,
    session_id       VARCHAR,
    song             VARCHAR,  
    status           VARCHAR, 
    ts               VARCHAR, 
    user_agent       VARCHAR,
    user_id          VARCHAR  
)
""")

staging_songs_table_create = (""" 
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           FLOAT,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    year                INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER     IDENTITY(0,1),
    start_time  TIMESTAMP   NOT NULL,
    user_id     VARCHAR     NOT NULL,
    level       VARCHAR,
    song_id     VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    session_id  VARCHAR NOT NULL,
    location    VARCHAR,
    user_agent  VARCHAR,
    PRIMARY KEY(songplay_id)


)

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR,
    PRIMARY KEY (user_id)

)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL,
    title   VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL,
    PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    latitude float ,
    longitude float,
    PRIMARY KEY(artist_id)
        
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP,
    hour INT,
    day INT, 
    week INT,
    month INT,
    year INT,
    weekday INT,
    PRIMARY KEY(start_time)
)
""")


# STAGING TABLES COPY

staging_events_copy = ("""
COPY staging_events FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2' FORMAT AS JSON 's3://udacity-dend/log_json_path.json'; 
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""
COPY staging_songs FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}' REGION 'us-west-2'  FORMAT AS JSON 'auto'; 
""").format(*config['IAM_ROLE'].values())

# TEMP TABLES INSERT

temp_songplay_table_insert = ("""
CREATE TABLE temp_songplays AS
SELECT
DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' start_time,
a.user_id, a.level, b.song_id, b.artist_id, a.session_id, a.location, a.user_agent
from staging_events a
inner join staging_songs b on a.artist = b.artist_name and a.song = b.title
WHERE a.page = 'NextSong'
    """)

temp_user_table_insert = ("""
CREATE TABLE temp_users AS
SELECT 
    user_id, first_name, last_name,
    gender, level 
    FROM staging_events WHERE USER_ID IS NOT NULL
    AND page = 'NextSong'
    """)

temp_song_table_insert = ("""
CREATE TABLE temp_songs AS
SELECT 
    DISTINCT a.song_id, a.title, a.artist_id,a.year, cast(b.length as float) as length
    FROM staging_songs a
    inner join staging_events b on a.title = b.Song
WHERE b.page = 'NextSong'
    """)

temp_artist_table_insert = ("""
CREATE TABLE temp_artists AS
select  
    DISTINCT a.artist_id, a.artist_name, b.location,
    a.artist_latitude, a.artist_longitude
FROM staging_songs a
   inner join staging_events b on a.title = b.Song
WHERE b.page = 'NextSong'
    """)


temp_time_table_insert = ("""
CREATE TABLE temp_time AS
select 
    start_time, 
    extract(hour from start_time)    as hour, 
    extract(day from start_time)     as day, 
    extract(week from start_time)    as week,
    extract(month from start_time)   as month, 
    extract(year from start_time)    as year, 
    extract(weekday from start_time) as weekday
    
FROM
(SELECT 
 DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' start_time 
 FROM STAGING_EVENTS WHERE page = 'NextSong') temp;
""")

# FINAL TABLES INSERT

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
FROM (
       SELECT
         ROW_NUMBER() OVER (PARTITION BY start_time order by start_time) AS r,
       t.*
       from temp_songplay t
     ) x
where x.r = 1
    """)

user_table_insert = ("""
INSERT INTO users 
SELECT 
    user_id, first_name, last_name,
    gender, level 
FROM (
       SELECT
         ROW_NUMBER() OVER (PARTITION BY user_id order by user_id) AS r,
       t.*
       from temp_users t
     ) x
where x.r = 1
    """)

song_table_insert = ("""
INSERT INTO songs
SELECT 
    song_id, title, artist_id,year, length
FROM (
       SELECT
         ROW_NUMBER() OVER (PARTITION BY song_id order by song_id) AS r,
       t.*
       from temp_songs t
     ) x
where x.r = 1
    """)

artist_table_insert = ("""
INSERT INTO artists
select  
    artist_id, artist_name, location,
    artist_latitude, artist_longitude
FROM (
       SELECT
         ROW_NUMBER() OVER (PARTITION BY artist_id order by artist_id) AS r,
       t.*
       from temp_artists t
     ) x
where x.r = 1
    """)


time_table_insert = ("""
INSERT INTO time
select 
    start_time, 
    hour, 
    day, 
    week,
    month, 
    year, 
    weekday
    
FROM (
       SELECT
         ROW_NUMBER() OVER (PARTITION BY start_time order by start_time) AS r,
       t.*
       from temp_time t
     ) x
where x.r = 1
""")




# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_small_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_temp_table_queries = [temp_songplay_table_drop, temp_user_table_drop, temp_song_table_drop, temp_artist_table_drop, temp_time_table_drop]
drop_small_table_queries = [songplay_table_drop, 
user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_temp_table_queries = [temp_songplay_table_insert, temp_user_table_insert, temp_song_table_insert, temp_artist_table_insert, temp_time_table_insert]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

