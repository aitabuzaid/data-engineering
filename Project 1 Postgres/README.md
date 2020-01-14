## Creating an ETL pipeline for Sparkify 
Author: Abdulrahman Abuzaid
Date: Jan 12, 2020

This project creates a postgres database to faciliate analyzing data collected on songs
and user activities for the a startup named Sparkify. The data is on different JSON files
and the aim of this project is to extract data from these files, transform it, and then load
it to the database, effectively creating an appropriate ETL pipeline.

The new database helps Sparkify answer their analytical questions such as:
 - Which song is most listened to?
 - Which user is using the app the most?
 - What is the percentage of users that have paid subscription?
 - Which artist is the most popular?
 - When is the rush hour for the app usage?
 
## Database Schema Design
The selected database design for this project is the star schema. The fact table is the
songplays table as it measures the usage of the app. The fact tables are the users table, 
artists table, songs table, and the time table. 

## Packages Used
 - pandas
 - psycopg2
 - os
 - glob
 
## Execution Guide
In order to run the ETL pipeline, execute the following code in the terminal:
-> python3 create_tables.py
-> python3 etl.py

If the program is successful, load the test.ipynb notebook in order to perform sample queries.

## Sample Queries
### Query 1
%sql SELECT songplays.user_id, users.first_name, users.last_name, users.level, COUNT(songplays.user_id)  \
FROM songplays JOIN users ON songplays.user_id = users.user_id \
GROUP BY songplays.user_id, users.first_name, users.last_name, users.level \
ORDER BY count DESC \
LIMIT 3

### Result

| user_id | first_name | last_name  |  level |  count |
|---------|------------|------------|--------|--------|
| 49      | Chloe      |   Cuevas   |  paid  |  689   |
| 80      | Tegan      |   Levine   |  paid  |  665   |
| 97      | kate       |   Harell   |  paid  |  557   |

### Query 2

%sql SELECT level, COUNT(user_id) FROM users GROUP BY level

### Result

| level | count |
|-------|-------|
| free  |  74   |
| paid  |  22   |

### Query 3

%sql SELECT gender, COUNT(user_id) FROM users GROUP BY gender

| gender | count |
|--------|-------|
| male   |  55   |
| female |  41   |
