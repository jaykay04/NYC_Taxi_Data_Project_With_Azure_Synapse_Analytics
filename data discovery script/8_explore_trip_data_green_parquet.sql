USE nyc_taxi_discovery;


SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

--lets identify ther inferred the data types and see how good they are
EXEC sp_describe_first_result_set N'SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''trip_data_green_parquet/year=2020/month=01/*.parquet'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''PARQUET''
    ) AS [result]'

--define column the data types
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )WITH(
        VendorID INT,
        lpep_pickup_datetime DATETIME2(7),
        lpep_dropoff_datetime DATETIME2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT
    ) AS [result]

--select only required fields
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )WITH(
        tip_amount FLOAT,
        trip_type INT
    ) AS [result]

--query from folders using wildcard characters
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )WITH(
        tip_amount FLOAT,
        trip_type INT
    ) AS [result]

--use filename function to get file names 
SELECT
    TOP 100 
    result.filename() AS file_name,
    result.*
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )WITH(
        tip_amount FLOAT,
        trip_type INT
    ) AS [result]

--use filepath functions
SELECT
    TOP 100 
    result.filename() AS file_name,
    result.filepath(1) AS year,
    result.filepath(2)AS month,
    result.*
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )WITH(
        tip_amount FLOAT,
        trip_type INT
    ) AS [result]

SELECT
    TOP 100 
    result.filepath(1) AS year,
    result.filepath(2)AS month,
    result.*
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )WITH(
        tip_amount FLOAT,
        trip_type INT
    ) AS [result]
    WHERE result.filepath(1) IN ('2020','2021')
        AND result.filepath(2) IN ('01','02','03');

--target partitions using wildcards
SELECT
    result.filepath(1) AS year,
    result.filepath(2) AS month,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
GROUP BY result.filepath(1), result.filepath(2)
HAVING result.filepath(1) = '2020' 
    AND result.filepath(2) IN ('06','07','08')
ORDER BY result.filepath(1), result.filepath(2);