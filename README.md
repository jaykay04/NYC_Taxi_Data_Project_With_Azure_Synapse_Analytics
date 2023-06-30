# NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics
This Project is about building a robust, scalable and resilient data pipeline for reporting and analysis of New York Taxi Data in other to make data driven decisions as regards how best to run campaigns and also help identify demands for taxis using the full power of Azure Synapse Analytics.

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
#### 4. Scheduling Requirments
* The Pipeline built should be scheduled to run at regualar intervals
* Ability to monitor the status of pipeline excution.
* We should be able to rerun failed pipelines and set up alerts on failures.
#### 5. Reporting Requirements
* Create BI reports to identify demands during days of the week including weekends  as well as in different locations.
* This report should also aid in making data driven decisions as regards campaigns aimed at encouraging credit card payments as opposed cash payments.
* Build capability for Operational reporting on the data from IOT devices in the taxis. This should be a Near Real time Analytics.
## Full Solution Architecture of this Azure Synapse Analytics Project
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/Overall%20Solution%20Architecture.png">

## Azure Services used for this project
*  Azure DataLake Storage Gen2
*  Azure Synpase Workspace
*  Serverless SQL Pool
*  Dedicated SQL Pool
*  Spark Pool
*  Synapse Pipelines
*  Power BI
*  Azure Data Explorer
*  Azure Data Studio.

## Overview of the Datasets used and their formats
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/Files%20Overview.png">

## Serverless SQL Pool Implementation of the Project
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

Note that the use of *STORED PROCEDURES* here to write the data into partitions is not the most effective approach.  Spark Pool is the best approach to impelement this and it will be demonstrated in the later section of this project.

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

### Pipeline Scheduling and Orchestration using Azure Synapse Pipelines
After all the scripts required to process the data from the bronze to silver and to the gold layer has been developed, we need to create pipelines to schedule these scripts so that they are regulated at regular interval while being able to monitor their executions and alerted fo errors.
The Tool or Service used basically for this purpose is the *Azure Synapse Pipeline*. 

*Azure Synapse Pipeline* is a fully managed serverless data integration and orchestration service made available within the Azure Synapse Studio.
In this Project, we worked with seven different datasets, while only one of them was partitioned, the trip data green dataset.
So, we created a single parameterized pipeline to dynamically ingest and transform the other six files without partitions so we don't have too many pipelines to manage. 

This was done by creating a pipeline variable which takes the names of all the six files and the folder paths to be processed in an array. We also created Stored Procedures for the CETAS statements that created the files.
A *ForEach* ativity was then used to iterate over the array which invoked a *delete activity* and *stored procedure activity* for each iteration.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/pl_create_silver_tables.png">
We also need to create a pipeline to ingest and transform the trip data green dataset because the file was written in partitions by year and month.
The first step was to use a *script activity* to get the disctinct year and month followed by a *ForEach activity* to iterate over each year and month.
Inside the *ForEach Activity*, we then call a *delete activity* and a *Stored Procedure activity* to create the file in partitions inside inside the silver layer of the storage. This was followed by another *script activity* to create a view of the data in the silver schema.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/pl_create_silver_trip_data_and_views.png">

We followed the same approach to create the aggregated trip data in partitions inside the gold layer storage together with a view of the data in the gold schema as shown below
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/pl_create_gold_trip_data_and_views.png">

Now, we have three pipelines, we need to create pipeline dependencies which will execute the three pipelines. This was done by creating a master pipeline and calling the *Execute Pipeline activity* which will run the Silver Tables Pipeline and the Silver Trip data green in parallel while the Gold trip data green pipeline will be dependent on the successful completion of the other pipelines before being executed. 
A schedule Trigger was then attached to the master pipeline to automate the whole process as shown below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/pl_master_pipeline.png">

### Spark Pool Implementation of the Transformation Logic of the Trip Data Green from the Silver Layer to the Gold/Reporting Layer
As stated earlier, writing partitioned data from source to destination using serverless sql pool is not the most efficient and effective approach, which brings us to Azure Synapse Spark Pool.

We want to demontrate how we can use the spark pool compute engine to easily write data in partitions into storage while also showing the seamless integration between *Spark Pool* and *Serverless Sql Pool*.

*Azure Synapse Spark Pool* is a managed Apache Spark compute engine that allows us to perform big data analytics and machine learning in the Synapse Studio. It is Optimized for big data preparation and transformation.

You can use the *Spark Pool* to Extract, Transform and Load data into storage while also creating a Spark Table in the Lake Database at the same time. 
Serverless SQL Pool can then read this data created in the Spark Table due to the seamless integration between the two services.
#### Spark and Serverless SQL Pool Architecture
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/spark_serveless1.png">
The Seamless integration between Spark and Serverless SQL Pool is shown below
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/spark_serverless2.png">

The first step to implement this part of the project is to spin up a spark pool and specify the number of nodes and create a notebook to write our transformation script as shown below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/spark1.png">

<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/spark2.png">

After the notebook script has been created, we need to automate the execution of the script by creating a synapse pipeline to automate the execution as shown below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/pl_spark1.png">

Finally, we then attached the pipeline to the master pipeline as shown in the image below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/pl_spark2.png">

## Creating Reports from the aggregated data in the gold layer to help make data driven decisions 
The final requirement of this project is to analyze the data and create reports that can help the company make data driven decisions as regards targeted campaigns and having a full understanding for the demands for taxis on a daily basis and per boroughs.

The first thing to do was to connect our Power BI desktop to the synapse workspace which gives access to all the tables and views that has been created.
We picked the required tables that meet these business requirements and start creating our reports.

Two reports was created.
The first was aimed at being able to target the right campaigns to encourage Credit card payments over Cash payments as shown below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/report1.png">

From the report above, we were able to deduce the following;
* Track the behaviour of Card and Cash payments over time from Jan 2020 to July 2021 using a line chart.
* We could observe that  there is a huge variation between card and cash payment from monday to thursday but it reduced on fridays, saturdays and sundays. This means that cash mode of payment increased during weekends. Therefore, we could target our campaigns for these days to yield maximum result.
* We could also see that for the boroughs, Manhattan and Brooklyn has more card payments than cash payments but other boroughs are closely matched except for Queens who has more cash payments than card payments. Therefore, we need to target our campaigns to the other boroughs that are closely matched while being more aggressive with our campaigns in Queens to achieve an optimal result.

The second report created was aimed at understanding the demands for taxis on a daily basis and per borough. 
We also want to understand the demands bases on trip type i.e Dispatch Vs Street-hail as shown below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/report2.png">

From the analysis above, we could deduce the following;
* There is an increasing demand for taxis especially on thursdays and fridays with fridays being the highest.
* Manhattan has has the most demands for taxis followed by Queens and Brooklyn while the other boroughs has a very low demand for taxis compared to theses boroughs.
* We could also see that we have more demands for Street-hail trip type compared to the dispatch.

## Dedicated SQL Pool as a Serving/Staging Layer
We could see from our implementations above that Azure Serverless SQL Pool is a very powerful tool for Data Exploration and also BI Reporting while the Spark pool is super great for Big Data transformations and analytics, but their draw back is that they do not provide storage.

We were able to connect to Serverless SQL pool from our BI tools via the creation of External tables or Views which are built on top of the files in the data lake storage, same for the Spark pools, the spark tables created can only be accessed via Serverless SQL pool also.

Therefore, we should be able to make the data available in some sort of physical tables in a datawarehouse. That is where *Dedicated SQL Pool* comes into play.

*Azure Dedicated SQL Pool* (Formerly known has Azure SQL Datawarehouse) is a distributed query engine that can be used to perform high performance Big Data Analytics using T-SQL.

It offers a storage solution where data can be stored in columnar format for query optimization by BI team. It is a datawarehouse that utilizes the Massively Parallel Processing (MPP) architecture.

### Solution Architecture of Dedicated SQL Pool Implementation
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/dedicated%20sql%20pool%20architecture.png">

We can easily perform data exploration and discovery using the Serverless SQL Pool and do complex transformations and aggregations using the Spark pool before staging the transformed/aggregated data in a serving layer i.e a Datawarehouse like the Dedicated SQL Pool has seen in the architecture above.

There are two methods we can use to make the data available in our datawarehouse (dedicated sql pool).

The First approach is to create an external table on the file using Dedicated SQL Pool. When this is done, we are basically reading the data from the files in data lake using polybase which utilizes the power of MPP architecture.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/external_table_dedicated_sql_pool.png">

After creating the external table, we can then write a CETAS statement to create an internal table and copy the data from the external table into it simultaneously as shown below
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/internal_table_dedicated_sql_pool.png">.

The second approach is using the copy command which is much more easy to use and recommended by microsoft as shown below;
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/copy%20to%20table%20dedicated%20sql%20pool.png">.

Finally, we can connect our Azure Data Studio for querying the data and Power BI where we can generate our report and publish to the power BI work space.
<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/connect%20to%20azure%20data%20studio.png">

<img src="https://github.com/jaykay04/NYC_Taxi_Data_Project_With_Azure_Synapse_Analytics/blob/main/Synapse%20Project%20Images/connect%20to%20power%20bi.png">

In conclusion, we could see how powerful Azure Synapse Analytics could be in the Extraction, Transformation and Loading of Big Data so as to be able to derive useful and actionable insights to drive business goals and profitability.


### Follow Me
* Linkedin: https://www.linkedin.com/in/joshua-gbegudu/
* Github: github.com/jaykay04
* Instagram: @Official_jaykay04
