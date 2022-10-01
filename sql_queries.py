import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_ROLE_ARN =config.get("IAM_ROLE","ARN")

JSONPATH =config.get("S3", "LOG_JSONPATH")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession int,
lastname varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId int,
song varchar,
status int,
ts timestamp,
userAgent varchar,
userId int)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year int) 
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time timestamp distkey,
user_id int,
level VARCHAR,
song_id varchar ,
artist_id varchar ,
session_id int,
location VARCHAR,
user_agent VARCHAR) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id int ,
first_name varchar,
last_name varchar,
gender varchar,
level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_id varchar ,
title varchar,
artist_id varchar ,
year int ,
duration FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id varchar,
name varchar,
location varchar,
latitude float,
longitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time timestamp distkey,
hour int,
day int,
week varchar,
month int,
year int ,
weekday varchar) DISTSTYLE KEY
""") 

# STAGING TABLES

staging_events_copy = (""" 
copy staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON {}
timeformat   as 'epochmillisecs';  
""").format(DWH_ROLE_ARN,JSONPATH) #timeformat  as 'epochmillisecs' Beacuse source data is represented as epoch time

staging_songs_copy = (""" 
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON 'auto';
""").format(DWH_ROLE_ARN)

# FINAL TABLES
songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP e.ts as start_time, 
        e.userId        as user_id, 
        e.level         as level, 
        s.song_id       as song_id, 
        s.artist_id     as artist_id, 
        e.sessionId     as session_id, 
        e.location      as location, 
        e.userAgent     as user_agent
    from staging_events e
    join staging_songs  s
    on e.song = s.title 
    and e.artist = s.artist_name 
    and e.page = 'NextSong' 
    and e.length = s.duration
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
    select
        distinct(userId)    as user_id,
        firstName           as first_name,
        lastName            as last_name,
        gender,
        level
    from staging_events
    where user_id is not null
    and page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs 
SELECT DISTINCT (song_id)
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists 
SELECT DISTINCT (artist_id)
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")


time_table_insert = ("""
INSERT INTO time
        SELECT DISTINCT ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM staging_events
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
