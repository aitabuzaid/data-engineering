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
