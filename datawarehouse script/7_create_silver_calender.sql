USE nyc_taxi_ldw
GO

IF OBJECT_ID('silver.calendar') IS NOT NULL
    DROP EXTERNAL TABLE silver.calendar

CREATE EXTERNAL TABLE silver.calendar
    WITH(
        LOCATION = 'silver/calendar',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT * 
    FROM bronze.calendar
GO

SELECT * FROM silver.calendar;