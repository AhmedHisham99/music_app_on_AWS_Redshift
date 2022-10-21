import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")


# DROP TABLES

staging_events_table_drop = "drop table if exists events_staging"
staging_songs_table_drop = "drop table if exists songs_staging"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists times"

# CREATE TABLES

staging_events_table_create= ("""
    create table events_staging (
        artist varchar,
        auth varchar not null,
        firstName varchar,
        gender char (1),
        itemInSession int not null,
        lastName varchar,
        length numeric,
        level varchar not null,
        location varchar,
        method varchar not null,
        page varchar not null,
        registration numeric,
        sessionId int not null,
        song varchar,
        status int not null,
        ts numeric not null,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    create table songs_staging (
        num_songs int not null,
        artist_id char (18) not null,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar not null,
        song_id char (18) not null,
        title varchar not null,
        duration numeric not null,
        year int not null
    )
""")

songplay_table_create = ("""
    create table songplays (
        songplay_id int identity(0, 1) primary key,
        start_time timestamp not null,
        user_id int not null,
        level varchar not null,
        song_id char (18),
        artist_id char (18),
        session_id int not null,
        location varchar,
        user_agent varchar not null
    )
""")

user_table_create = ("""
    create table users (
        user_id int primary key,
        first_name varchar not null,
        last_name varchar not null,
        gender char (1) not null,
        level varchar not null
    )
""")

song_table_create = ("""
    create table songs (
        song_id char (18) primary key,
        title varchar not null,
        artist_id char (18) not null,
        year int not null,
        duration numeric not null
    )
""")

artist_table_create = ("""
    create table artists (
        artist_id char (18) primary key,
        name varchar not null,
        location varchar,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    create table times (
        start_time timestamp primary key,
        hour int not null,
        day int not null,
        week int not null,
        month int not null,
        year int not null,
        weekday int not null
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy events_staging from {}
    iam_role '{}'
    format as json {}
""").format(
    config.get("S3", "LOG_DATA"),
    ARN,
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
    copy songs_staging from {}
    iam_role '{}'
    json 'auto'
""").format(config.get("S3", "SONG_DATA"), ARN)

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays(
        start_time, user_id, level, song_id, artist_id,
        session_id, location, user_agent
    )
    select
        timestamp 'epoch' + event.ts / 1000 * interval '1 second' as start_time,
        event.userId as user_id,
        event.level,
        song.song_id,
        song.artist_id,
        event.sessionId as session_id,
        event.location,
        event.userAgent as user_agent
    from events_staging event
    left join songs_staging song on event.song = song.title and event.artist = song.artist_name
    where event.page = 'NextSong'

""")

user_table_insert = ("""
    insert into users
    select event_user.userId, event_user.firstName, event_user.lastName, event_user.gender, event_user.level
    from events_staging event_user
    join (
        select max(ts) as ts, userId
        from events_staging
        where page = 'NextSong'
        group by userId
    ) usrid on event_user.userId = usrid.userId and event_user.ts = usrid.ts
""")
song_table_insert = ("""
    insert into songs
    select
        song_id,
        title,
        artist_id,
        year,
        duration
    from songs_staging
""")

artist_table_insert = ("""
    insert into artists
    select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from songs_staging
""")

time_table_insert = ("""
    insert into times
    select
        timetable.start_time,
        extract(hour from timetable.start_time) as hour,
        extract(day from timetable.start_time) as day,
        extract(week from timetable.start_time) as week,
        extract(month from timetable.start_time) as month,
        extract(year from timetable.start_time) as year,
        extract(weekday from timetable.start_time) as weekday
    from (
        select distinct
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        from events_staging
        where page = 'NextSong'
    ) timetable
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
