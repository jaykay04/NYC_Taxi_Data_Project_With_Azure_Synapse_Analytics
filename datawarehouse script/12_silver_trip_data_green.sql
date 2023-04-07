USE nyc_taxi_ldw
GO

/*
IF OBJECT_ID('silver.trip_data_green') IS NOT NULL 
    DROP EXTERNAL TABLE silver.trip_data_green

CREATE EXTERNAL TABLE silver.trip_data_green
    WITH(
        LOCATION = 'silver/trip_data_green',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT * 
    FROM bronze.trip_data_green_csv
GO

SELECT TOP 100 * FROM silver.trip_data_green;
*/



--Execute the Stored Procedure
EXEC silver.usp_silver_trip_data_green '2020', '01'
EXEC silver.usp_silver_trip_data_green '2020', '02'
EXEC silver.usp_silver_trip_data_green '2020', '03'
EXEC silver.usp_silver_trip_data_green '2020', '04'
EXEC silver.usp_silver_trip_data_green '2020', '05'
EXEC silver.usp_silver_trip_data_green '2020', '06'
EXEC silver.usp_silver_trip_data_green '2020', '07'
EXEC silver.usp_silver_trip_data_green '2020', '08'
EXEC silver.usp_silver_trip_data_green '2020', '09'
EXEC silver.usp_silver_trip_data_green '2020', '10'
EXEC silver.usp_silver_trip_data_green '2020', '11'
EXEC silver.usp_silver_trip_data_green '2020', '12'
EXEC silver.usp_silver_trip_data_green '2021', '01'
EXEC silver.usp_silver_trip_data_green '2021', '02'
EXEC silver.usp_silver_trip_data_green '2021', '03'
EXEC silver.usp_silver_trip_data_green '2021', '04'
EXEC silver.usp_silver_trip_data_green '2021', '05'
EXEC silver.usp_silver_trip_data_green '2021', '06'











