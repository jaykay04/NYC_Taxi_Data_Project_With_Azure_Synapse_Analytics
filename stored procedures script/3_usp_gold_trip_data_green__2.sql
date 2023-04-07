USE nyc_taxi_ldw
GO

CREATE OR ALTER PROCEDURE gold.usp_gold_trip_data_green2
    @year VARCHAR(4),
    @month VARCHAR(2)
AS
BEGIN

    DECLARE @create_sql_statement NVARCHAR(MAX),
            @drop_sql_statement NVARCHAR(MAX);
    
    SET @create_sql_statement =
        'CREATE EXTERNAL TABLE gold.trip_data_green_2_' + @year + '_' + @month +
        ' WITH(
            DATA_SOURCE = nyc_taxi_src,
            LOCATION = ''gold/trip_data_green2/year=' + @year + '/month=' + @month + ''',
            FILE_FORMAT = parquet_file_format
        )
        AS
        SELECT td.year, 
        td.month,
        tz.borough,
        CONVERT(DATE, td.lpep_pickup_datetime) trip_date,
        cal.day_name trip_day,
        CASE WHEN cal.day_name IN (''Saturday'', ''Sunday'') THEN ''Y'' ELSE ''N'' END AS trip_day_wknd_ind,
        SUM(CASE WHEN pt.description = ''Credit card'' THEN 1 ELSE 0 END) card_trip_count,
        SUM(CASE WHEN pt.description = ''Cash'' THEN 1 ELSE 0 END) cash_trip_count,
        SUM(CASE WHEN tt.trip_type_desc = ''Street-hail'' THEN 1 ELSE 0 END) street_hail_trip_count,
        SUM(CASE WHEN tt.trip_type_desc = ''Dispatch'' THEN 1 ELSE 0 END) dispatch_trip_count,
        SUM(td.trip_distance) trip_distance,
        SUM(DATEDIFF(MINUTE, td.lpep_pickup_datetime, td.lpep_dropoff_datetime)) trip_duration,
        SUM(td.fare_amount) fare_amount
    FROM silver.vw_trip_data_green td
    JOIN silver.taxi_zone tz ON (td.PULocationID = tz.location_id)
    JOIN silver.calendar cal ON (cal.date = CONVERT(DATE, td.lpep_pickup_datetime))
    JOIN silver.payment_type pt ON (td.payment_type = pt.payment_type)
    JOIN silver.trip_type tt ON (td.trip_type = tt.trip_type)
        WHERE td.year = ''' + @year + '''
          AND td.month = ''' + @month + '''
    GROUP BY td.year,
            td.month,
            tz.borough,
            CONVERT(DATE, td.lpep_pickup_datetime),
            cal.day_name ';

    print(@create_sql_statement)
    EXEC sp_executesql @create_sql_statement;

    SET @drop_sql_statement = 
        'DROP EXTERNAL TABLE gold.trip_data_green_2_' + @year + '_' + @month;

    print(@drop_sql_statement)
    EXEC sp_executesql @drop_sql_statement;
END;