## Creating an ETL pipeline for Sparkify Using Apache Cassandra with Query Optimization
Author: Abdulrahman Abuzaid

Date: Jan 15, 2020

This project creates an Apache Cassandra database to faciliate analyzing data collected on songs
and user activities for the a music streaming app 'Sparkify'. The data is on different CSV files
and the aim of this project is to extract data from these files, transform it by merging it into one CSV file, 
and finally load it to the NoSQL database that is optimized to answer the following questions:

The new database helps Sparkify answer the three questions below:
1. Provide the artist, song title and song's length in the music app history during sessionId = 338, and itemInSession = 4

2. Provide the name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, and sessionid = 182

3. Who are the users (provide first and last name) in the music app history that listened to the song 'All Hands Against His Own'

 
## Database Design
Since Sparkify is interested in three different questions, the queries should be designed to answer these questions optimally. 
A NoSQL database is warranted, and in this scenario we will use a Cassandra database with the following primary keys. Justification
for using these primary keys is available in the Jupyter notebook.

### Query 1
 - Partition Key: session_id
 - Clustering columns: item_in_session
 
### Query 2
 - Partition Key: user_id
 - Clustering columns: session_id, item_in_session
 
### Query 3
 - Partition Key: song_title
 - Clustering columns: session_id, item_in_session

## Packages Used
 - cassandra
 - os
 - glob
 - csv
 
## Execution Guide
Part 1 of theJupyter notebook is concerned with extracting and transforming the different csv files into one csv file. While part 2 creates
Cassandra tables and loads the data into the three tables.

## Sample Queries
### Query 1
SELECT artist_name, song_title, song_length FROM song_play_session WHERE session_id = 338 AND item_in_session = 4

### Result
Faithless Music Matters (Mark Knight Dub) 495.30731201171875


### Query 2
SELECT artist_name, song_title, item_in_session, first_name, last_name \
         FROM song_play_user \
         WHERE user_id = 10 AND session_id = 182 \
         ORDER BY session_id, item_in_session

### Result
Down To The Bone Keep On Keepin' On 0 Sylvie Cruz
Three Drives Greece 2000 1 Sylvie Cruz
Sebastien Tellier Kilometer 2 Sylvie Cruz
Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit) 3 Sylvie Cruz

### Query 3
SELECT first_name, last_name \
         FROM song_play_song \
         WHERE song_title = 'All Hands Against His Own'

### Result                                         
Sara Johnson
Jacqueline Lynch
Tegan Levine                               

