USE nyc_taxi_ldw
GO

CREATE OR ALTER PROCEDURE silver.usp_silver_trip_data_green
    @year VARCHAR(4),
    @month VARCHAR(2)
AS
BEGIN

    DECLARE @create_sql_statement NVARCHAR(MAX),
            @drop_sql_statement NVARCHAR(MAX);
    
    SET @create_sql_statement =
        'CREATE EXTERNAL TABLE silver.trip_data_green_' + @year + '_' + @month +
        ' WITH(
            DATA_SOURCE = nyc_taxi_src,
            LOCATION = ''silver/trip_data_green/year=' + @year + '/month=' + @month + ''',
            FILE_FORMAT = parquet_file_format
        )
        AS
        SELECT [VendorID] 
                ,[lpep_pickup_datetime]
                ,[lpep_dropoff_datetime]
                ,[store_and_fwd_flag]
                ,[total_amount]
                ,[payment_type]
                ,[trip_type]
                ,[congestion_surcharge]
                ,[extra]
                ,[mta_tax]
                ,[tip_amount]
                ,[tolls_amount]
                ,[ehail_fee]
                ,[improvement_surcharge]
                ,[RatecodeID]
                ,[PULocationID]
                ,[DOLocationID]
                ,[passenger_count]
                ,[trip_distance]
                ,[fare_amount]
        FROM
        bronze.vw_trip_data_green
        WHERE year = ''' + @year + '''
          AND month = ''' + @month + '''';

    print(@create_sql_statement)
    EXEC sp_executesql @create_sql_statement;

    SET @drop_sql_statement = 
        'DROP EXTERNAL TABLE silver.trip_data_green_' + @year + '_' + @month;

    print(@drop_sql_statement)
    EXEC sp_executesql @drop_sql_statement;
END;