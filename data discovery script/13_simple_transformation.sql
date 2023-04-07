USE nyc_taxi_discovery

--Number of trips made by duration in minutes

SELECT
    DATEDIFF(minute, lpep_pickup_datetime,lpep_dropoff_datetime)/60 from_hour,
    (DATEDIFF(minute, lpep_pickup_datetime,lpep_dropoff_datetime)/60)+1 to_hour,
    COUNT(1) number_of_trips
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data
GROUP BY DATEDIFF(minute, lpep_pickup_datetime,lpep_dropoff_datetime)/60, 
        (DATEDIFF(minute, lpep_pickup_datetime,lpep_dropoff_datetime)/60)+1        
ORDER BY number_of_trips DESC;