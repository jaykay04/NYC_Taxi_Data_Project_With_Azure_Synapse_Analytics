USE nyc_taxi_discovery;

SELECT  
    TOP 100 *
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=2020/month=01/',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green;

--Select data from subfolders
SELECT  
    TOP 100 *
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=2020/month=*/',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green;

SELECT  
    TOP 100 *
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=2020/**',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green;

--Get data from jan, feb and march alone
SELECT  
    TOP 100 *
FROM OPENROWSET(
    BULK ('trip_data_green_csv/year=2020/month=01/',
            'trip_data_green_csv/year=2020/month=02/',
            'trip_data_green_csv/year=2020/month=03/'),
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green;

--Get data from all the years and all the months and only csv files
SELECT  
    TOP 100 *
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green;

--Get file metadata from filename function
SELECT  
    TOP 100 
    trip_data_green.filename() AS file_name,
    trip_data_green.*
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green;

-- Get the number of records for each file name
SELECT  
    trip_data_green.filename() AS file_name,
    COUNT(1) AS record_count
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green
GROUP BY trip_data_green.filename()
ORDER BY trip_data_green.filename();

--We can use filenames to target partitions as well
SELECT  
    trip_data_green.filename() AS file_name,
    COUNT(1) AS record_count
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green
GROUP BY trip_data_green.filename()
HAVING trip_data_green.filename() IN ('green_tripdata_2020-01.csv','green_tripdata_2021-01.csv')
ORDER BY trip_data_green.filename();

--lets use the filepath function as well
SELECT  
    trip_data_green.filename() AS file_name,
    trip_data_green.filepath() AS file_path,
    COUNT(1) AS record_count
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green
GROUP BY trip_data_green.filename(), trip_data_green.filepath()
HAVING trip_data_green.filename() IN ('green_tripdata_2020-01.csv','green_tripdata_2021-01.csv')
ORDER BY trip_data_green.filename(), trip_data_green.filepath();

--We can use the filepath function to get the years and month 
SELECT  
    trip_data_green.filename() AS file_name,
    trip_data_green.filepath() AS file_path,
    trip_data_green.filepath(1) AS year,
    trip_data_green.filepath(2) AS month,
    COUNT(1) AS record_count
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green
GROUP BY trip_data_green.filename(), trip_data_green.filepath(),trip_data_green.filepath(1),trip_data_green.filepath(2)
HAVING trip_data_green.filename() IN ('green_tripdata_2020-01.csv','green_tripdata_2021-01.csv')
ORDER BY trip_data_green.filename(), trip_data_green.filepath(),trip_data_green.filepath(1),trip_data_green.filepath(2);

--Lets see how many trips where made each month in each year using the filepath function
SELECT  
    trip_data_green.filepath(1) AS year,
    trip_data_green.filepath(2) AS month,
    COUNT(1) AS record_count
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green
GROUP BY trip_data_green.filepath(1),trip_data_green.filepath(2)
ORDER BY trip_data_green.filepath(1),trip_data_green.filepath(2);

--Lets use the filepath function to target partitions
SELECT  
    trip_data_green.filepath(1) AS year,
    trip_data_green.filepath(2) AS month,
    COUNT(1) AS record_count
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green
GROUP BY trip_data_green.filepath(1),trip_data_green.filepath(2)
HAVING trip_data_green.filepath(1) = '2020' AND trip_data_green.filepath(2) IN ('06','07','08')
ORDER BY trip_data_green.filepath(1),trip_data_green.filepath(2);

SELECT  
    trip_data_green.filepath(1) AS year,
    trip_data_green.filepath(2) AS month,
    COUNT(1) AS record_count
FROM OPENROWSET(
    BULK 'trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) AS trip_data_green
GROUP BY trip_data_green.filepath(1),trip_data_green.filepath(2)
HAVING trip_data_green.filepath(1) IN ('2020','2021') 
    AND trip_data_green.filepath(2) IN ('06','07','08')
ORDER BY trip_data_green.filepath(1),trip_data_green.filepath(2);
