## Creating an ETL pipeline for Sparkify Using Apache Cassandra with Query Optimization
Author: Abdulrahman Abuzaid
Date: Jan 15, 2020

This project creates an Apache Cassandra database to faciliate analyzing data collected on songs
and user activities for the a music streaming app called Sparkify. The data is on different CSV files
and the aim of this project is to extract data from these files, transform it by merging it into one CSV file, 
and finally load it to the NoSQL database that is optimized to answer the following questions:

The new database helps Sparkify answer the three questions below:
1. Provide the artist, song title and song's length in the music app history during sessionId = x, and itemInSession = y

2. Provide the name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, and sessionid = 182

3. Who are the users (provide first and last name) in the music app history that listened to the song 'All Hands Against His Own'

 
## Database Schema Design


## Packages Used
 - cassandra
 - os
 - glob
 - csv
 
## Execution Guide


## Sample Queries
### Query 1


### Result



### Query 2



### Result



### Query 3


