USE nyc_taxi_discovery;

--Identify number of trips made from each borough
-- To be able to achieve this, we need to join trip data with taxi zone
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

--check if the PULocationID does not have a null
SELECT
    COUNT(1) total_records,
    COUNT(PULocationID) total_locationId_record
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

--OR
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE PULocationID IS NULL

--Join trip data and taxi zone 
SELECT taxi_zone.borough, COUNT(1) number_of_trips
    FROM OPENROWSET(
                        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
                        DATA_SOURCE = 'nyc_taxi_data_raw',
                        FORMAT = 'PARQUET'
    ) AS trip_data
JOIN 
    OPENROWSET(
                        BULK 'taxi_zone.csv',
                        DATA_SOURCE = 'nyc_taxi_data_raw',
                        FORMAT = 'CSV',
                        PARSER_VERSION = '2.0',
                        FIRSTROW = 2
                    ) 
                    WITH (
                        location_id SMALLINT 1,
                        borough VARCHAR(15) 2,
                        zone VARCHAR(50) 3,
                        service_zone VARCHAR(15) 4
    )AS taxi_zone
ON trip_data.PULocationID = taxi_zone.location_id
GROUP BY taxi_zone.borough
ORDER BY number_of_trips DESC;