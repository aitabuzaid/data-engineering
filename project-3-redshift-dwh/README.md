## Creating a Data Warehouse for Sparkify Music App Using AWS Redshift 
Author: Abdulrahman Abuzaid
Date: Feb , 2020

This project creates a data warehouse for the Sparkify music app using AWS (Amazon Web Services) Redshift. The redshift cluster reads JSON files from two sources: log data for the app and song data. The program reads the data into two corresponding stage tables and subsequently loads the data into the final tables, which as created using the star schema as explained in the following section.

The new database helps Sparkify answer their analytical questions such as:
 - Which song is most listened to?
 - Which user is using the app the most?
 - What is the percentage of users that have paid subscription?
 - Which artist is the most popular?
 - When is the rush hour for the app usage?
 
## Data Warehouse Table Design
The selected database design for this project is the star schema. The fact table is the songplays table as it measures the usage activity of the app. The fact tables are the users table, artists table, songs table, and the time table. The tables have the following sort keys and distribution styles in order to optimize the query performance.
 - songplays fact table:
   - sort key:   start_time
   - dist style: key
   - dist key:   user_id, since it is expected to join on the user_id in order to provide recommendations for the users and have user                  centric analysis.
 - users dimension table: 
   - sort key:   user_id
   - dist style: key
   - dist key:   user_id, since it is expected to join on the user_id in order to provide recommendations for the users and have user                  centric analysis.
 - songs dimension table:
   - sort key:   song_id
   - dist style: all, since the table is assumed to be small
 - artists dimension table:
   - sort key:   artist_id
   - dist style: all, since the table is assumed to be small
 - time dimension table:
   - sort key:   start_time
   - dist style: even, since the table is assumed to be as big as the songplays fact table

## Packages Used
 - pandas
 - psycopg2
 - json
 - boto3
 - configparser
 
## Execution Guide
In order to run the ETL pipeline, execute the following code in the terminal:
-> python3 manager_cluster.py
   Choose option 1 to create cluster, and wait until cluster is created, choose option 2 to check
   the status of the cluster
-> python3 create_tables.py
-> python3 etl.py
   If the program is successful, load the test.ipynb notebook in order to perform sample queries.
   
-> python3 manage_cluster.py
   Choose option 3 to delete the cluster once finished

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
| 49      | Chloe      |   Cuevas   |  paid  |  42    |
| 80      | Tegan      |   Levine   |  free  |  42    |
| 97      | kate       |   Harell   |  paid  |  32    |

### Query 2

%sql SELECT level, COUNT(user_id) FROM users GROUP BY level

### Result

| level | count |
|-------|-------|
| free  |  83   |
| paid  |  22   |

### Query 3

%sql SELECT gender, COUNT(user_id) FROM users GROUP BY gender

| gender | count |
|--------|-------|
| male   |  45   |
| female |  60   |
