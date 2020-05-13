# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int NOT NULL,
    first_name VARCHAR NOT NULL, last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    PRIMARY KEY(user_id))
    """)

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL,
    PRIMARY KEY(song_id))
    """)

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR,
    name VARCHAR NOT NULL,
    location VARCHAR, 
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY(artist_id))
    """)

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time bigint,
    hour int,
    day int, 
    week int,
    month int,
    year int,
    weekday int,
    PRIMARY KEY(start_time))
    """)

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id SERIAL,
    start_time bigint NOT NULL REFERENCES time(start_time),
    user_id INT NOT NULL REFERENCES users(user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR  REFERENCES songs (song_id),
    artist_id VARCHAR  REFERENCES artists(artist_id), 
    session_id INT NOT NULL, 
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL,
    PRIMARY KEY(start_time, user_id))
    """)


# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,session_id,location,user_agent,song_id, artist_id)
    values (%s,%s,%s,%s,%s,%s,%s,%s)  ON CONFLICT DO NOTHING
    """)

user_table_insert = ("""
INSERT INTO users(user_id,first_name,last_name,gender,level)
    values(%s,%s,%s,%s,%s)  ON CONFLICT (user_id) DO UPDATE SET level = users.level
    """)

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
    values(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
    """)

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
    values(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
    """)


time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT 
    s.song_id, a.artist_id
FROM artists a 
join songs s on a.artist_id = s.artist_id
where a.name = %s and s.title = %s and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]