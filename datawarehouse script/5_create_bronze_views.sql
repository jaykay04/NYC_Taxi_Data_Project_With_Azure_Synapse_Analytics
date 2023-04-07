USE nyc_taxi_ldw
GO

--create rate_code view
DROP VIEW IF EXISTS bronze.vw_rate_code
GO

CREATE VIEW  bronze.vw_rate_code
AS
SELECT rate_code_id, rate_code
    FROM OPENROWSET(
        BULK 'raw/rate_code.json',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b' --specifically for standard json
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    ) AS rate_code
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(20)
    )
GO

SELECT * FROM bronze.vw_rate_code
GO

--create payment_type view
DROP VIEW IF EXISTS bronze.vw_payment_type
GO

CREATE VIEW  bronze.vw_payment_type
AS
SELECT payment_type, description
    FROM OPENROWSET(
        BULK 'raw/payment_type.json',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        description VARCHAR(15) '$.payment_type_desc'
    )
GO

SELECT * FROM bronze.vw_payment_type
GO

--create view for trip_data_green to target partitions
DROP VIEW IF EXISTS bronze.vw_trip_data_green
GO

CREATE VIEW bronze.vw_trip_data_green
AS
SELECT  
    trip_data_green.filepath(1) AS year,
    trip_data_green.filepath(2) AS month,
    trip_data_green.*
FROM OPENROWSET(
    BULK 'raw/trip_data_green_csv/year=*/month=*/*.csv',
    DATA_SOURCE = 'nyc_taxi_src',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0'
) 
WITH(
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
        )
AS trip_data_green
GO

SELECT TOP 100 * FROM bronze.vw_trip_data_green
WHERE year = '2020' AND month IN ('06', '07', '08')
GO