USE nyc_taxi_ldw
GO

--create view for trip_data_green
DROP VIEW IF EXISTS silver.vw_trip_data_green
GO

CREATE VIEW silver.vw_trip_data_green
AS
SELECT  
    trip_data_green.filepath(1) AS year,
    trip_data_green.filepath(2) AS month,
    trip_data_green.*
FROM OPENROWSET(
    BULK 'silver/trip_data_green/year=*/month=*/*.parquet',
    DATA_SOURCE = 'nyc_taxi_src',
    FORMAT = 'PARQUET'
) 
WITH(
        vendor_id INT,
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
        )
AS trip_data_green
GO