import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP \
                                TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP \
                                TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP \
                            TABLE IF EXISTS songplays"
user_table_drop = "DROP \
                        TABLE IF EXISTS users"
song_table_drop = "DROP \
                        TABLE IF EXISTS songs"
artist_table_drop = "DROP \
                        TABLE IF EXISTS artists"
time_table_drop = "DROP \
                        TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                                CREATE TABLE staging_events
                                    (                                
                                        artist_name VARCHAR(255),
                                        auth VARCHAR(50),
                                        user_first_name VARCHAR(255),
                                        user_gender  VARCHAR(1),
                                        item_in_session INTEGER,
                                        user_last_name VARCHAR(255),
                                        song_length DOUBLE PRECISION, 
                                        user_level VARCHAR(50),
                                        location VARCHAR(255),
                                        method VARCHAR(25),
                                        page VARCHAR(35),
                                        registration VARCHAR(50),
                                        session_id BIGINT,
                                        song_title VARCHAR(255),
                                        status INTEGER, 
                                        ts VARCHAR(50),
                                        user_agent TEXT,
                                        user_id VARCHAR(100)                                      
                                    )
                            """)

staging_songs_table_create = ("""
                                CREATE TABLE staging_songs
                                    (
                                        song_id VARCHAR(100),
                                        num_songs INTEGER,
                                        artist_id VARCHAR(100),
                                        artist_latitude DOUBLE PRECISION,
                                        artist_longitude DOUBLE PRECISION,
                                        artist_location VARCHAR(255),
                                        artist_name VARCHAR(255),
                                        title VARCHAR(255),
                                        duration DOUBLE PRECISION,
                                        year INTEGER
                                    )
                            """)

songplay_table_create = ("""
                            CREATE TABLE songplays
                                (
                                    songplay_id INT IDENTITY(0,1),
                                    start_time TIMESTAMP NOT NULL,
                                    user_id VARCHAR(50) NOT NULL,
                                    level VARCHAR(50) NOT NULL,
                                    song_id VARCHAR(100),
                                    artist_id VARCHAR(100),
                                    session_id BIGINT NOT NULL,
                                    location VARCHAR(255),
                                    user_agent TEXT NOT NULL,
                                    PRIMARY KEY (songplay_id)
                                )
                        """)

user_table_create = ("""
                        CREATE TABLE users
                            (
                                user_id VARCHAR,
                                first_name VARCHAR(255) NOT NULL,
                                last_name VARCHAR(255) NOT NULL,
                                gender VARCHAR(1),
                                level VARCHAR(50) NOT NULL,
                                PRIMARY KEY (user_id)
                            )
                    """)

song_table_create = ("""
                        CREATE TABLE songs
                            (
                                song_id VARCHAR(100),
                                title VARCHAR(255) NOT NULL,
                                artist_id VARCHAR(100) NOT NULL,
                                year INTEGER,
                                duration DOUBLE PRECISION NOT NULL,
                                PRIMARY KEY (song_id)
                            )
                    """)

artist_table_create = ("""
                            CREATE TABLE artists
                                (
                                    artist_id VARCHAR(100),
                                    name VARCHAR(255) NOT NULL,
                                    location VARCHAR(255),
                                    latitude DOUBLE PRECISION,
                                    longitude DOUBLE PRECISION,
                                    PRIMARY KEY (artist_id)
                                )
                        """)

time_table_create = ("""
                        CREATE TABLE time
                            (
                                start_time TIMESTAMP,
                                hour INTEGER,
                                day INTEGER,
                                week INTEGER,
                                month INTEGER,
                                year INTEGER,
                                weekday INTEGER,
                                PRIMARY KEY (start_time)
                            )
                    """)

# STAGING TABLES

staging_events_copy = ("""COPY staging_events 
                          from {}
                          iam_role {}
                          FORMAT AS json {}
                          REGION 'us-west-2';
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs 
                          from {}
                          iam_role {}
                          FORMAT AS json 'auto'
                          REGION 'us-west-2';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays 
                                (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                                SELECT  
                                    TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
                                    e.user_id, 
                                    e.user_level, 
                                    s.song_id,
                                    s.artist_id, 
                                    e.session_id,
                                    e.location, 
                                    e.user_agent
                                    FROM staging_events e, staging_songs s
                                    WHERE e.page = 'NextSong' 
                                        AND e.song_title = s.title 
                                        AND e.artist_name = s.artist_name 
                                        AND e.song_length = s.duration
                        """)

user_table_insert = ("""
                        INSERT INTO users (user_id, first_name, last_name, gender, level)
                        WITH unique_staging_events_user AS (
                            SELECT user_id, first_name, last_name, gender, level, 
                                ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
                                FROM staging_events
                                WHERE userid IS NOT NULL AND page = 'NextSong'
                        )
                        SELECT user_id, first_name, last_name, gender, level
                            FROM unique_staging_events_user
                            WHERE rank = 1;
                    """)

song_table_insert = ("""
                        INSERT INTO songs (song_id, title, artist_id, year, duration) 
                            SELECT DISTINCT 
                                song_id, 
                                title,
                                artist_id,
                                year,
                                duration
                                FROM staging_songs
                                WHERE song_id IS NOT NULL
                    """)

artist_table_insert = ("""
                            INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                                SELECT DISTINCT 
                                    artist_id,
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude
                                    FROM staging_songs
                                    WHERE artist_id IS NOT NULL
                        """)

time_table_insert = ("""
                        INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
                        WITH unique_staging_events_time AS (
                            SELECT start_time,
                                EXTRACT(hour FROM start_time),
                                EXTRACT(day FROM start_time),
                                EXTRACT(week FROM start_time), 
                                EXTRACT(month FROM start_time),
                                EXTRACT(year FROM start_time), 
                                EXTRACT(dayofweek FROM start_time)
                                ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
                                FROM songplays
                                WHERE start_time IS NOT NULL
                        )
                        SELECT start_time, hour, day, week, month, year, weekDay
                            FROM unique_staging_events_time
                            WHERE rank = 1;

                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
