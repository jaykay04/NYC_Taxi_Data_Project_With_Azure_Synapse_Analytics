USE nyc_taxi_ldw
GO

IF OBJECT_ID('silver.payment_type') IS NOT NULL 
    DROP EXTERNAL TABLE silver.payment_type

CREATE EXTERNAL TABLE silver.payment_type
    WITH(
        LOCATION = 'silver/payment_type',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT * 
    FROM bronze.vw_payment_type
GO

SELECT * FROM silver.payment_type;