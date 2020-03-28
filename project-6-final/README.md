
# Data Lake for US Immigration Data Using Spark 
### Data Engineering Capstone Project

#### Project Summary
This project uses immigration data to the United States, combined with three other data sources, namely, historical averages of weather temperatures, airports in the US, and demographics of US cities in order to create a data lake through Spark that is ready for analysis when required. The project will provide a data model in a star schema with one fact and three dimensional tables. 

### Step 1: Scope the Project and Gather Data

#### Scope 
The project will combine four data sources: I94 Immigration Data, World Temperature Data, US City Demographic Data, and Airport Code Table. I will use Spark in order to build a data lake and model the data in a star schema, with the I94 immigration data as the fact table and all other data sources as the dimension tables. The tables will be saved as parquet files as the end solution will be a set of parquet files that can be loaded whenever a new analysis is needed by the Data Science team. The only tool that will be used in this project is Apache Spark. Amazon EMR and Apache Airflow will not be used in this project, however, the project can be extended later on to include these technologies once the processing requirements increase. 

#### Describe and Gather Data 
Describe the data sets you're using. Where did it come from? What type of information is included? 

The fact table is the immigration data from the US National Tourism and Trade Office, detailing the respondents information (people travelling into the US), while the dimension tables are as follows: the first provides the average temperature for cities in the world and it is provided by Kaggle datasets, the second provides detailed information about city airports in the US, and it is provided from DataHub, while the last provides further details about demographics in US cities, and it is provided from OpenSoft. The three data sets will be joined through taking cities as the primary key.

Sample queries that the final data model will be able to answer:

 -How does immigration differ in terms of visa type and the origin country? 
 
 -Is there any effect of temperature on the number of people visiting a city? 
 
 -What is the correlation between the type of airport and the number of people visiting a city? 
 
 -Do people form certain geographical areas are more likely to visit US cities with certain demographics? 

### Step 2: Explore and Assess the Data

#### Exploring and Cleaning the Data
The immigration data is the largest out of the four data sources, and it is the most challenging to clean. Based on the problems I want to solve and specified in part 1 of this project, I decided to drop eight columns that are either irrelevant or have a lot of missing values. The dropped columns and the number of non-null values in each are as follows: 

- visapost:  1215063
- occup:        8126
- entdepa:   3096075
- entdepd:   2957884
- entdepu:       392
- matflag:   2957884
- dtaddto:   3095836
- insnum:     113708




Moreover, the weather data is huge and has around 8 million records. Each record has the average monthly temperature for one city. The city spans more than 250 years and for cities from all over the world. We need to clean the data and group it so that it only captures monthly averages across all years and only for US cities. This will provide a way for us to join the temperature data with the immigration data. 

Finally, the airport and demographic data is clean already and it only requires column renaming in order to follow the same naming convention among all tables.

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
As breifly mentioned earlier, the data model used is the star schema with the immigration data in the center of the star and acts as the facts table, while the other three tables are dimension tables that provide further information about immigration. The three dimension tables are the airports table, the temperature table, and the demographics table all on the city level. All four tables are joined together using a unique city id (using the IATA code as proxy for a unique ID). 

#### 3.2 Mapping Out Data Pipelines
I will list the steps necessary for the data pipeline to work correctly for each table.
##### Fact immigration table
 - Load the immigration data
 - Drop the unnecessary columns
 - Rename the column names to follow the convention
 - Cast the columns into the correct data types
 - Join the data with the airports data using the IATA airport code
 - Recreate the immigration table with the same columns and add the city name column as well
 
##### Dimension airport table
 - Load the airport data
 - Drop the unnecessary columns
 - Rename the column names to follow the convention
 - The columns are already in the correct data types
 - Join the data with the immigration data using the city id (IATA airport code)
 - Recreate the airports table with the same columns after filtering to select the matched records only
 - Drop the duplicates from the table to have unique records
 
##### Dimension temperature table
 - Load the temperature data
 - Rename the column names to follow the convention
 - The columns are already in the correct data types
 - Filter the data for only the month of April and for US cities
 - Group the data by city and calculate the average temperature across all years in the sample
 - Join the data with the immigration data using the city id (IATA airport code)
 - Recreate the temperature table with the same columns after filtering to select the matched records only
 - Drop the duplicates from the table to have unique records
 
##### Dimension demographics table
 - Load the demographics data
 - Rename the column names to follow the convention
 - Cast the columns into the correct data types
 - Join the data with the immigration data using the city id (IATA airport code)
 - Recreate the demographics table with the same columns after filtering to select the matched records only
 - Drop the duplicates from the table to have unique records
 

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Data Quality: To ensure data quality, I made sure that all the dimension tables have unique values in the city_id and city columns. Moreover, I also checked that there are no null values in the main columns due to unmatched data. The code for this part can be seen in the ETL above as the last step before writing the tables as parquet files. Moreover, the below data quality checks show that the number of records in each table to ensure we have a positive number of records in each table.

#### 4.3 Data dictionary 

The data follows a star schema with one fact table and three dimensional tables. 
#### Fact table: immigration 
##### Data Fields: 
-	id: a unique id for each entry (respondent). 
-	year: the year in which the respondent entered the US (all the data is for 2016). 
-	month: the month in which the respondent entered the US (all the data is for April). 
-	origin_country_id: and id for the country of origin of the respondent. The SAS description file provides the key-value pairs for these countries
-	city_id: the US port city, the city at which the respondent entered the US. This column will be used to join the data with the airport data to get the city name. 
-	arrival_date: provides the date on which the respondent entered the US, includes year/month/day. 
-	transport_mode: a code to identify the mode of transport for the respondent: sea, air, land. 
-	departure_date: a date field showing the respondent’s departure date from the US, if departed already. 
-	visa_purpose: the purpose of the visa: business, study, tourism. 
-	visa_type: the US visa type: B1, B2, F1, and so on. 
-	birth_year: the year of birth of the respondent. 
-	gender: the gender of the respondent. 
-	admin_number: the administration number for each entry and for each respondent.
-	airline_code: a code for the airline organization used by the respondent to travel, if travelling by air.
-	flight_number: the flight number that the respondent travelled in, if travelling by air. 

#### Dimension Tables: 
#### Table 1: airports
##### Fields: 
-	iata_code: a unique code for each airport consisting from three letters. 
-	name: name of the airport. 
-	type: type of the airport; small, medium, large, etc. 
-	city: the name of the city in which the airport is located. 
-	region: the region in which the airport is located, it is mostly equivalent to the US state. -	country: the country of the airport, in this case, it is the US. 
-	continent: the continent in which the airport is located: North America. 
-	coordinates: the coordinate of the airport in northing and easting. 
-	gps_code: a code for the location that is relevant for GPS systems.
-	local_code: a code for the airport as per US conventions. 

#### Table 2: temperatures 
##### Fields:
-	city_id: a unique ID for each city, it is equivalent for the port_city_id in the facts table. 
-	city: the name of the city.
-	latitude: the latitude of the city. 
-	longitude: the longitude of the city. 
-	avg_temp: average monthly temperature of the city across all the years available. Original data has years 1750-2013. Since all arrivals are for April, then these averages are all for April. 

#### Table 3: demographics 
##### Fields:
-	city_id: a unique ID for each city, it is equivalent for the port_city_id in the facts table. 
-	city: the name of the city.
-	median_age: median age of all people living in the city. 
-	male_population: number of male residents in the city. 
-	female_population: number of female residents in the city. 
-	total_population: number of total population male and female. 
-	number_of_veteran: number of veterans in the city. 
-	number_of_foreign_born: number of foreign-born residents. 
-	household_size: the average household size in the city. 
-	state_code: the two-letter code for the state in which the city is located. 
-	race: the majority race in the city. Explore Data: In this part, I will provide the count of each value for each data field. This will help us to understand the data and what it represents. 


#### Step 5: Complete Project Write Up

In this project, I chose to use Apache Spark for processing the data since I am planning to develope a data lake for others to use by loading data and analyizing it. Apache Spark can be easily deployed on an Amazon EMR cluster in order to scale processing resources as deemded necessary, the original data is only for the month of April 2016 and it is already in the scale of 2-3 millions. Once this data pipeline is deployed on the cluster, the need for additional processing capability is guaranteed in order to cope with all the data; for example, one year worth of data could be in the range of 50 million rows. 

Depending on the team's objective, the data could be updated daily, weekly, or monthly. This depends on what kind of decisions the immigration department within the US is taking. For example, policy decisions do not change abruptly and a weekly or monthly update could be appropriate; however, admissibility of individuals is more critical and a daily update could be warranted.

Recommendations for the different scenarios:
- In case the data was increased by one hundred times, I would scale the number of Apache Spark clusters by a factor of 10-100 depending on how often I need to update the data.
- In case the data needs to be populated in a dashboard on a daily basis, I would use Apache Airflow to manage the data pipeline through a DAG (Directed Acyclic Graph) that runs with a service level of agreement of 24 hours. I would also use enough workers in the DAG to ensure that the SLA is met.
- If the database needs to be accessed by 100 people, I would first ensure that my Spark cluster (for example, Amazon EMR) is configured with the right IAM (Identity Access Management) roles and user groups so that the 100 have access. Moreover, I would ensure that the cluster has enough workers to serve all the users with an acceptable latency.
