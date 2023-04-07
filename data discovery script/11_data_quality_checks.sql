USE nyc_taxi_discovery;

--Identify any data quality issues in trip total amount
--check the min,max and average amount including null values
SELECT
    MIN(total_amount) min_total_amount,
    MAX(total_amount) max_total_amount,
    AVG(total_amount) avg_total_amount,
    COUNT(1) total_number_of_records,
    COUNT(total_amount) not_null_total_records
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

