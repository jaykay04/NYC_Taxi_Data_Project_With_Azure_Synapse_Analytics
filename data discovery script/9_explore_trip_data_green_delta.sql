USE nyc_taxi_discovery;

SELECT TOP 100 *
    FROM OPENROWSET(
        BULK 'trip_data_green_delta',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) AS trip_data_delta;

--chekc the inferred data types
EXEC sp_describe_first_result_set N'SELECT TOP 100 *
    FROM OPENROWSET(
        BULK ''trip_data_green_delta'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''DELTA''
    ) AS trip_data_delta';

--define the data types
SELECT TOP 100 *
    FROM OPENROWSET(
        BULK 'trip_data_green_delta',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) WITH(
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
        congestion_surcharge FLOAT,
        year CHAR(4),
        month CHAR(2)
    )AS trip_data_delta;

-- We can also select required columns only
SELECT TOP 100 *
    FROM OPENROWSET(
        BULK 'trip_data_green_delta',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) WITH(
        tip_amount FLOAT,
        trip_type INT,
        year CHAR(4),
        month CHAR(2)
    )AS trip_data_delta;

--We can also target partitions here
SELECT year,
        month,
        COUNT(1) record_count
    FROM OPENROWSET(
        BULK 'trip_data_green_delta',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) AS trip_data_delta
GROUP BY year, month
ORDER BY year, month;