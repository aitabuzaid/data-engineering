## Creating a Data Lake for Sparkify Music App Using Spark Run on AWS EMR and S3 
Author: Abdulrahman Abuzaid
Date: Feb 29, 2020


The new data lake helps Sparkify answer analyze their data by raising questions such as:
 - Which song is most listened to?
 - Which user is using the app the most?
 - What is the percentage of users that have paid subscription?
 - Which artist is the most popular?
 - When is the rush hour for the app usage?
 


## Packages Used
 - pyspark
 - datetime
 - os
 
## Execution Guide
 - Create an EMR cluster on AWS
 - Create a new 'pem' key pair or use an existing one
 - Configure the cluster to be accessed through SSH (enable port 22)
 - If using Windows: Use PUTTYGen to convert the 'pem' into 'pkk' key pair 
 - Connect (SSH into) to the cluster. PUTTY is a great option for Windows
 - Create an S3 folder for output files and the python scrip file (use same region as EMR)
 - Upload the etl.py scrip into your S3 folder
 - Run command in EMR console:
 
     --> spark-submit --master yarn S3://your-bucket/etl.py
 - Once the program finishes, confirm that parquet files are save in five separate folders.

