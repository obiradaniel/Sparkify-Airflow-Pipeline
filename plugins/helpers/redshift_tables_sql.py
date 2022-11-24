
# TABLES SQL FROM COURSE 2: Cloud Data Warehouses


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events"
staging_songs_table_drop = "DROP TABLE IF EXISTS songlogs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events 
    (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length REAL,
        level VARCHAR(4),
        location VARCHAR,
        method VARCHAR(6),
        page VARCHAR(20),
        registration BIGINT,
        sessionId INT,
        song VARCHAR(MAX),
        status INT,
        ts BIGINT,
        userAgent VARCHAR(MAX),
        userId INT
    );                              
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songlogs
    (
        artist_id VARCHAR, 
        artist_latitude FLOAT,
        artist_location VARCHAR(MAX),
        artist_longitude FLOAT,
        artist_name VARCHAR(MAX),
        duration REAL,
        num_songs INT,
        song_id VARCHAR, 
        title VARCHAR(MAX), 
        year INT 
    );                               
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
    (
        songplay_id BIGINT IDENTITY(1,1), 
        start_time TIMESTAMP DISTKEY SORTKEY, 
        user_id INT, 
        level VARCHAR(4) NOT NULL, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR(MAX),
        PRIMARY KEY (songplay_id, start_time, user_id)
    );            
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
    (
        user_id INT PRIMARY KEY DISTKEY SORTKEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR(1) NOT NULL, 
        level VARCHAR(4) NOT NULL
    );                     
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
    (
        song_id VARCHAR PRIMARY KEY DISTKEY, 
        title VARCHAR(MAX) NOT NULL, 
        artist_id VARCHAR NOT NULL SORTKEY, 
        year INT, 
        duration REAL NOT NULL
    );                     
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
    (
        artist_id VARCHAR PRIMARY KEY DISTKEY SORTKEY, 
        name VARCHAR(MAX) NOT NULL, 
        location VARCHAR(MAX),
        latitude FLOAT,
        longitude FLOAT
    );                       
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
    (
        start_time TIMESTAMP PRIMARY KEY DISTKEY SORTKEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month VARCHAR NOT NULL,
        year INT NOT NULL,
        weekday VARCHAR NOT NULL
    );                
""")

# STAGING TABLES

staging_events_copy = ("""
COPY events
from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
compupdate off
json {};                     
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), \
    config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
COPY songlogs
from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
compupdate off
json 'auto';                     
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
    (start_time ,user_id, level, song_id, artist_id, session_id, 
     location, user_agent)
    SELECT
            TIMESTAMP 'epoch' + (ev.ts/1000) * INTERVAL '1 second' AS start_time, 
            ev.userId, 
            ev.level, 
            sl.song_id, 
            sl.artist_id, 
            ev.sessionId, 
            ev.location, 
            ev.userAgent
    FROM
        events ev 
        JOIN 
        songlogs sl
        ON ((ev.song = sl.title ) AND (ev.song = sl.title ) AND ((ev.length - sl.duration)<0.1))  
        WHERE (ev.page = 'NextSong') AND (sl.song_id IS NOT NULL) AND (ev.userId IS NOT NULL);                  
""")

user_table_insert = ("""
INSERT INTO users 
    (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM
        events
        WHERE (page = 'NextSong') AND (userId IS NOT NULL);

""")

song_table_insert = ("""
INSERT INTO songs 
    (song_id, title, artist_id, year, duration)        
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        songlogs
""")

artist_table_insert = ("""
INSERT INTO artists 
    (artist_id, name, location, latitude, longitude)        
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        songlogs                     
""")

time_table_insert = ("""
INSERT INTO time 
    (start_time, hour, day, week, month, year, weekday)        
    SELECT DISTINCT 
        start_time,
        EXTRACT(HOUR FROM start_time),
        EXTRACT(DAY FROM start_time),
        EXTRACT(WEEK FROM start_time),
        TO_CHAR(start_time, 'Month'),
        EXTRACT(YEAR FROM start_time),
        TO_CHAR(start_time, 'Day')
    FROM
        songplays                   
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
