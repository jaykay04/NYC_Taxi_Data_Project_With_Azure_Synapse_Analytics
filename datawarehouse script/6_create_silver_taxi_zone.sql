USE nyc_taxi_ldw;

--create external tables in silver layer using CETAS(Create External Table As Select)

--Create taxi_zone table
IF OBJECT_ID('silver.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE silver.taxi_zone
GO
    
CREATE EXTERNAL TABLE silver.taxi_zone
    WITH(
        LOCATION = 'silver/taxi_zone',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT * 
    FROM bronze.taxi_zone;

SELECT TOP 100 * FROM silver.taxi_zone;