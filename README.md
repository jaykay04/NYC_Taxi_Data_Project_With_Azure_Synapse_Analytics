# NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics
This Project is about building scalable and resilient data pipeline for reporting and analysis of New York Taxi Data in other to be able to make data driven decisions as regards how best to run campaigns and also help identify demands for taxis using the full power of Azure Synapse.

## What is Azure Synapse Analytics
Azure Synapse Analytics is a limitless analytics service that brings together data integration, enterprise datawarehousing and big data analytics.
It is a single window where you can do data ingestion, transformations and visulaization. It also provides storage as well as developmemnt, monitoring, managememnt and security capabilities.

## Project Requirements
The requirements for the execution of this project will be broken down into 5 different segements with each segment hahving its own unique statement problems.
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
* The Pipeline built should be schedules to run at regualr intervals
* Ability to monitor the status of pipeline excution.
* We should be able to rerun failed pipelines and set up alerts on failures.
