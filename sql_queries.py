import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"


staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events ( 
    event_id int IDENTITY(0, 1),
    artist text,
    auth text,
    firstName text,
    gender varchar(1),
    itemInSession integer,
    lastName text,
    length float,
    level text,
    location text,
    method text,
    page text,
    registration varchar(12),
    sessionId integer,
    song text,
    status text,
    ts bigint,
    userAgent text,
    userId integer);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer,
    artist_id text,
    artist_latitude float,
    artist_longitude float,
    artitst_location text,
    artitst_name text,
    song_id text,
    title text,
    duration float,
    year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1), 
    start_time timestamp not null sortkey distkey, 
    user_id numeric NOT NULL, 
    level text, 
    song_id text NOT NULL, 
    artist_id text NOT NULL, 
    session_id numeric, 
    location text, 
    user_agent text);
""")

user_table_create = (""" 
    CREATE TABLE IF NOT EXISTS users (
    user_id text PRIMARY KEY sortkey, 
    first_name text, 
    last_name text, 
    gender varchar(1), 
    level text);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year int sortkey,
    duration numeric);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY sortkey, 
    name text, 
    location text, 
    latitude float, 
    longitude float);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL PRIMARY KEY sortkey, 
    hour int,
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int);
""")

staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
COMPUPDATE OFF
JSON {};
""").format(config.get("S3","LOG_DATA"), config.get('IAM_ROLE', 'ARN'), config.get("S3","LOG_JSONPATH"))
    
staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""").format(config.get("S3","SONG_DATA"), config.get('IAM_ROLE', 'ARN'))

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
        userId as user_id,
        level as level,
        song_id as song_id
        artist_id as artist_id,
        sessionId as session_id,
        location as location,
        userAgent as user_agent
FROM staging_songs ss
JOIN staging_events se 
ON (ss.artist_name = se.artist AND ss.title = se.song)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT se.userId as user_id, 
        se.firstName as first_name, 
        se.lastName as last_name, 
        se.gender as gender, 
        se.level as level
FROM staging_events se
WHERE se.user_id NOT IN (SELECT DISTINCT user_id FROM se WHERE s.level = se.level);
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id as song_id, 
        title as title, 
        artist_id as artist_id, 
        year as year, 
        duration as duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
FROM staging_songs
WHERE staging_songs.artist_id NOT IN (SELECT DISTINCT staging_songs.artist_id FROM songplays s WHERE s.level = se.level);
ON CONFLICT (artist_id)
DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
SELECT start_time,
        EXTRACT(hour from start_time) as hour,
        EXTRACT(day from start_time) as day,
        EXTRACT(week from start_time) as week,
        EXTRACT(month from start_time) as month,
        EXTRACT(year from start_time) as year,
        EXTRACT(dayofweek from start_time) as weekday
FROM songsplay;
""")

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
