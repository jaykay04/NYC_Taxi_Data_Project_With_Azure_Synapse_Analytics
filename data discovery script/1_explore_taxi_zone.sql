-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://jaykayksynapseprojectdl.dfs.core.windows.net/nyc-taxi-data/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result]

--We can use the abfss protocol too instead of the https
--Note for Azure Blob, we use the wasps protocol
-- We can aslo specify the header row, field terminator and row terminator arguments
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

-- Examine the Data Types of the columns
EXEC sp_describe_first_result_set N'SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
        
    ) AS [result]'

-- Lets see the maximum lenght of each column
SELECT
    MAX(LEN(LocationID)) AS len_LocationId,
    MAX(LEN(Borough)) AS len_Borough,
    MAX(LEN(Zone)) AS len_Zone,
    MAX(LEN(service_zone)) AS len_service_zone
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

--We define the data types using the WITH clause
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone VARCHAR(15)
    )AS [result]

--Check the data types again
EXEC sp_describe_first_result_set N'SELECT
    *
FROM
    OPENROWSET(
        BULK ''abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''\n''
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone VARCHAR(15)
    )AS [result]'

--Lets find the default collation for the database
SELECT name, collation_name FROM sys.databases;

--We can specify UTF-8 collation in the WITH clause or on the entire database
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        Zone VARCHAR(50) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        service_zone VARCHAR(15) COLLATE Latin1_General_100_CI_AI_SC_UTF8
    )AS [result]

--Apply collation at the database level
CREATE DATABASE nyc_taxi_discovery;

USE nyc_taxi_discovery;
ALTER DATABASE nyc_taxi_discovery COLLATE Latin1_General_100_CI_AI_SC_UTF8;

SELECT name, collation_name FROM sys.databases;

--Check the collation applied on the databse
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone VARCHAR(15)
    )AS [result]

--Select only certain columns from the dataset
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        Borough VARCHAR(15),
        Zone VARCHAR(50)
    )AS [result]

--Read data from a file without headers
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone_without_header.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone VARCHAR(15)
    )AS [result]
--Select a subset of columns with no header name, we specify the header in the with clause using the ordinal positions as well
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone_without_header.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        Borough VARCHAR(15) 2,
        Zone VARCHAR(50) 3
    )AS [result]

-- We can adjust the column naming convention using the ordinal position as well
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/taxi_zone_without_header.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )AS [result]

--Create External data source that points to the end point
CREATE EXTERNAL DATA SOURCE nyc_taxi_data
WITH(
    LOCATION = 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/'
)

--Lets use the external data source created in our select
SELECT
    *
FROM
    OPENROWSET(
        BULK 'raw/taxi_zone_without_header.csv',
        DATA_SOURCE = 'nyc_taxi_data',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )AS [result]

--Create another data source up to the raw folder
CREATE EXTERNAL DATA SOURCE nyc_taxi_data_raw
WITH(
    LOCATION = 'abfss://nyc-taxi-data@jaykayksynapseprojectdl.dfs.core.windows.net/raw/'
)

--Lets use it in our select
SELECT
    *
FROM
    OPENROWSET(
        BULK 'taxi_zone_without_header.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )AS [result]

--lets drop one data source
DROP EXTERNAL DATA SOURCE nyc_taxi_data;

--we can check which storage our data source is pointing to 
SELECT name, location FROM sys.external_data_sources;
