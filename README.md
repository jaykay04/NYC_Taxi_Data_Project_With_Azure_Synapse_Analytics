# NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics
This Project is about building scalable and resilient data pipeline for reporting and analysis of New York Taxi Data in other to be able to make data driven decisions as regards how best to run campaigns and also help identify demands for taxis using the full power of Azure Synapse.

## What is Azure Synapse Analytics
Azure Synapse Analytics is a limitless analytics service that brings together data integration, enterprise datawarehousing and big data analytics.
It is a single window where you can do data ingestion, transformations and visualization. It also provides storage as well as developmemnt, monitoring, managememnt and security capabilities.

## Project Requirements
The requirements for the execution of this project will be broken down into 5 different segements with each segment having its own unique statement problems.
#### 1. Data Discovery Requiremetns
* We should be able to explore the raw data set to ensure data quality and integrity.
* Data Analysts should be able to also explore the data, which means schema on data needs to be applied. This will enhance understanding of the dataset and extract busineess value from them.
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
## Full Solution Architecture of this Azure Synapse Analytics Project
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/Overall%20Solution%20Architecture.png">

## Azure Services used for this project
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
Azure Serverless SQL Pool is a serverless distributed query engine that can be used to query data over the data lake using T-SQL.
There is no infrastructure to provision and clusters to administer.

### Data Discovery in the raw/bronze layer
As regards the project requirements, the first task is to use serverless SQL Pool to perform data discovery and exploration and this can be done using the *OPENROWSET* function in serverless sql pool.

The *OPENROWSET* function allows us to query the files directly from the storage as if it was a table.
The following data discovery and exploration was performed using the *OPENROWSET* function as shown below;

* checking for duplicates
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/check_duplicates.png">
* volume of data and data quality checks by checking for missing values, max, min, avg etc
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/data_quality_checks.png">
* perform joins on keys and also simple transformations to get business value from data
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/join_datasets.png">
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/simple_tranformation.png">

The challenges faced with the *OPENROWSET* functions include the following;
* We have to specify the storage account, file details and also file formats anytime we use the *OPENROWSET* function.
* The Columns names and data types has to be defined each time we query the data for cost optimization.
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
Now that external tables and views has been created in the bronze layer, the next step is to ingest the data from the bronze layer to the silver layer while also converting the CSV and JSON files into parquet so that analytical queries can run faster.
The approach used to implement this is the *CETAS* (Create External Table As Select).
What the *CETAS* does basically is to write the data to storage in the format you specify (we converted to parquet here) while also creating an External table at the same time as shown below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/cetas1.png">
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/cetas2.png">

One of the Limitation of the *CETAS* statement is that we can't write data directly into partitions.Data is only written into the one folder that was specified.
Therefore, we implemented the use of *STORED PROCEDURES* to create seperate tables for each partition so that we can force each of the tables to be written in a seperate folders in the container as shown below.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/sp1.png">
The Stored Procedures is then executed dynamically to give us partitioned data in the container.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/exec_sp.png">

After we have achieved the goal of writing the data into partitions, we then created a view on top of the data in the silver layer.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/data_trip_green_view.png">

Note that the use of *STORED PROCEDURES* here to write the data into partitions is not the most effective approach.  Spark Pool is the best approach to impelement this as shown below.

### Transform data from Silver layer into gold/reporting layer to meet business requirements
We have successfully moved data from the raw/bronze layer to the silver layer and also created external tables and views for easy access for data analysts and scientists. 
The next stage of the project involves transforming and aggregating the data into the gold/reporting layer to meet the business requirements.
We have some couple of business requirements which are summarized below;
#### 1. We want to be able to run campaigns to encourage credit card payments
This can be achieved by
* Tracking the number of trips making use of both card and cash payments
* Knowing the payment behaviour during days of the week and also weekend
* Monitoring the payment behaviour between boroughs
We also have some couple of non functional requirements which includes;
* Reporting data to be pre-aggregated for better performance
* Pre-aggregate data for each year/month partition in isolation
* Able to read data efficiently for specific months from aggregated data
#### 2. We want to identify the demands for taxis
This can be achieved by knowing;
* Demands based on borough
* Demand based on day of week/weekend
* Demand based on trip type (i.e Street hail/Dispatch)
* Trip distance, trip durations, total fare amount etc per day/borough

To satisfy these requirements, we joined five datasets together i.e the trip_data, taxi_zone, payment_type, trip_type and calendar data to achieve the aggregation and transformation logic needed to meet the requirements.
Joining these datasets to achieve the aggregation required was simple because we already created external tables and views on top of all these data in the silver layer which made it straight forward to just write *SELECT* statement to join the datasets and aggregate it by the *GROUP BY* clause inside a *CETAS* statement, then create a *STORED PROCEDURE* so that we can execute it dynamically.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/sp_gold2.png">
The *STORED PROCEDURE*  was now executed using the *EXEC* command
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/exec_sp_gold2.png">

Now that we have the aggregated data in the gold/reporting layer of our storage, we have to make it accessible for Data/BI Analysts so they can connect their various reporting tool conveniently, thus, we created a View on top of the aggregated data in the gold/reporting layer.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/view_gold2.png">
