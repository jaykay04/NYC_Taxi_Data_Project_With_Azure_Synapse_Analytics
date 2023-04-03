# NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics
This Project is about building scalable and resilient data pipeline for reporting and analysis of New York Taxi Data in other to be able to make data driven decisions as regards how best to run campaigns and also help identify demands for taxis using the full power of Azure Synapse.

## What is Azure Synapse Analytics
Azure Synapse Analytics is a limitless analytics service that brings together data integration, enterprise datawarehousing and big data analytics.
It is a single window where you can do data ingestion, transformations and visulaization. It also provides storage as well as developmemnt, monitoring, managememnt and security capabilities.

## Project Requirements
The requirements for the execution of this project will be broken down into 5 different segements with each segment having its own unique statement problems.
#### 1. Data Discovery Requiremetns
* We should be able to explore the raw data set to ensure data quality and integrity.
* Data Analysts should be able to to also explore the data, which means schema on data needs to be applied. This will enhance understanding of the dataset and extract busineess value from them.
* Data Analyst should be able to explore the data and query them using T-SQL.
#### 2. Data Ingestion Requirements
* Ingested data should be stored in columnar file format like parquet for better performance with analytical queries.
* Apply the right schemas to the data including column name and data types and stored in tables/views.
* The data should be easily accessed using T-SQL and other query engines such as spark and dataflows.
* Ingestion should be done using pay per query model - serverless SQL Pool rather than Dedicated resources.
#### 3. Data Transformation
* Join the key data required for reporting to create a new table so that we can produce BI reports from them.
* This transformed data must be easily available for Data Analysts using T-SQL and reporting using BI tools.
* Transformed data should be stored in columnar format such as parquet.
#### 4. Reporting Requirements
* Create BI reports to identify demands during days of the week including weekends  as well as in different locations.
* This report should also be able to make data driven decisions as regards campaigns aimed at encouraging credit card payments as opposed cash payments.
* Build capability for Operational reporting on the data from IOT devices in the taxis. This should be a Near Real time Analytics.
#### 5. Scheduling Requirments
* The Pipeline built should be scheduled to run at regualar intervals
* Ability to monitor the status of pipeline excution.
* We should be able to rerun failed pipelines and set up alerts on failures.
## Full Solution Architecture of an Azure Synapse Analytics Workspace
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/Overall%20Solution%20Architecture.png">

## Azure Services used for these project
*  Azure DataLake Storage Gen2
*  Azure Synpase Workspace
*  Serverless SQL Pool
*  Dedicated SQL Pool
*  Spark Pool
*  Synapse Link for Cosmos DB
*  Cosmos DB
*  Synapse Pipelines
*  Power BI
*  Azure Data Explorer
*  Azure Data Studio.

## Overview of the Datasets used and their formats
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/Files%20Overview.png">

## Serverless SQL Implementation of the Project
### Solution Architecture Serverless SQL Pool
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/Slution%20Architecture-Serverless%20SQL%20Pool.png">
Azure Serverless SQL Pool is a serverless distributed query engine that can be used to query data over the the data lake using T-SQL.
There is no infrastructure to provision and clusters to administer.

### Data Discovery in the raw/bronze layer
As regards the project requirements, the first task is to use serverless SQL Pool to perform data discovery and exploration and this can be done using the *OPENROWSET* function in serverless sql pool.

The *OPENROWSET* function allows us to be able to query the files directly from the storage as if it was a table.
The following data discovery and exploration was performed a shown below using the *OPENROWSET* function.

* checking for duplicates
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/check_duplicates.png">
* volume of data and data quality checks by checking for missing values, max, min, avg etc
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/data_quality_checks.png">
* perform joins on keys and also simple transformations to get business value from data
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/join_datasets.png">
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/simple_tranformation.png">

The challenges faced with the *OPENROWSET* functions include the following;
* We have to specify the storage account, file details and also file formats anytime we use the *OPENROWSET* function.
* The Columns names and data types has to be defined each time we query the data.
* There is lots of code repetition
* We can't query or connect the data using BI tools for reports 
Therefore, we want our data consumers which are the Analysts and Scientists to be able to access the files as though it was a Table or View which give them the ability to also connect using their BI tools to draw insights without having to worry about the location and file formats specified in the *OPENROWSET* function.
To achieve this, we created *EXTERNAL TABLES* and *VIEWS* on top of the files in the bronze layer as shown below.
#### External tables in the bronze layer
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/external_table1.png">
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/external_table2.png">

#### Views created in the bronze layer
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/views1.png">
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/views2.png">

### Data Ingestion and Transformation from the Bronze to the Silver Layer
Now that we have external tables and views has been created in the bronze layer, the next step is to ingest the data from the bronze layer to the silver layer while also converting the CSV and JSON files into parquet so that analytical queries can run faster.
The approach used to implement this is the *CETAS* (Create External Table As Select) as shown in below
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/cetas1.png">
