USE nyc_taxi_ldw
GO

--create external tables in silver layer using CETAS(Create External Table As Select)
--Then wrap it in a stored procedure

--Create taxi_zone table
CREATE OR ALTER PROCEDURE silver.usp_silver_taxi_zone
AS
BEGIN
IF OBJECT_ID('silver.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE silver.taxi_zone;
    
CREATE EXTERNAL TABLE silver.taxi_zone
    WITH(
        LOCATION = 'silver/taxi_zone',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT * 
    FROM bronze.taxi_zone;
END;