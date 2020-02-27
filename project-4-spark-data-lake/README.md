## Creating a Data Lake for Sparkify Music App Using Spark Run on AWS EMR and S3 
Author: Abdulrahman Abuzaid
Date: Feb , 2020


The new database helps Sparkify answer their analytical questions such as:
 - Which song is most listened to?
 - Which user is using the app the most?
 - What is the percentage of users that have paid subscription?
 - Which artist is the most popular?
 - When is the rush hour for the app usage?
 



## Packages Used

 
## Execution Guide


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
