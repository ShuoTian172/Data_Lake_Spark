from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date

# Staging tables schema
staging_songs_schema = R([
    Fld("song_id", Str()),
    Fld("num_songs", Int()),
    Fld("title", Str()),
    Fld("artist_name", Str()),
    Fld("artist_latitude", Dbl()),
    Fld("year", Int()),
    Fld("duration", Dbl()),
    Fld("artist_id", Str()),
    Fld("artist_longitude", Str()),
    Fld("artist_location", Str())
])

staging_events_schema = R([
    Fld("artist", Str()),
    Fld("auth", Str()),
    Fld("firstName", Str()), 
    Fld("gender", Str()), 
    Fld("itemInSession", Str()), 
    Fld("lastName", Str()), 
    Fld("length", Str()), 
    Fld("level", Str()), 
    Fld("location", Str()), 
    Fld("method", Str()), 
    Fld("page", Str()), 
    Fld("registration", Str()), 
    Fld("sessionId", Str()), 
    Fld("song", Str()), 
    Fld("status", Str()), 
    Fld("ts", Str()), 
    Fld("userAgent", Str()), 
    Fld("userId", Str()) 
])

#Form table queries
select_users = """
SELECT 
    DISTINCT userId, firstName, lastName, gender, level 
FROM 
    (SELECT 
        userId, firstName, lastName, gender, level , row_number() over(partition by userId order by firstName desc) 
    AS sequence from staging_events ) a 
WHERE sequence=1 
"""

select_songs = """
SELECT 
    DISTINCT song_id, title, artist_id, year, duration 
FROM 
    (SELECT 
        song_id, title, artist_id, year, duration , ROW_NUMBER() OVER(partition by song_id order by title desc) AS sequence 
    FROM 
        staging_songs ) 
WHERE sequence =1   
"""

select_artists = """
SELECT 
    DISTINCT artist_id, name, location, latitude, longitude
FROM  
    (SELECT 
        artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, 
        artist_longitude AS longitude, row_number() over(partition by artist_id order by artist_location desc) AS  sequence 
    FROM staging_songs) 
WHERE 
    sequence =1 
"""

select_time = """
SELECT  
     DISTINCT from_unixtime(cast(ts as bigint)/1000,'yyyy-MM-dd HH:mm:sss') AS start_time
FROM
    staging_events
"""

select_songplays = """
SELECT 
    distinct from_unixtime(cast(ts as bigint)/1000,'yyyy-MM-dd HH:mm:sss') AS start_time, se.userId, se.level, tmp.song_id, tmp.artist_id, se.sessionId, se.location, se.userAgent  FROM staging_events as se 
INNER JOIN
    (SELECT 
        songs.song_id, artists.artist_id, songs.title, songs.duration, artists.name FROM songs
    INNER JOIN 
        artists
    ON 
        songs.artist_id = artists.artist_id) 
    AS tmp
ON 
    tmp.title=se.song AND tmp.name=se.artist AND tmp.duration=se.length
WHERE 
    se.page='NextSong'
"""
