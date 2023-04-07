USE nyc_taxi_ldw
GO

IF OBJECT_ID('silver.trip_type') IS NOT NULL
    DROP EXTERNAL TABLE silver.trip_type

CREATE EXTERNAL TABLE silver.trip_type
    WITH(
        LOCATION = 'silver/trip_type',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT * 
    FROM bronze.trip_type
GO

SELECT * FROM silver.trip_type;