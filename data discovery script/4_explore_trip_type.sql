USE nyc_taxi_discovery;

SELECT 
    *
    FROM OPENROWSET(
        BULK 'trip_type.tsv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FORMAT = 'CSV',
        FIELDTERMINATOR = '\t'
    ) AS trip_type

SELECT 
    *
    FROM OPENROWSET(
        BULK 'trip_type.tsv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FORMAT = 'CSV',
        FIELDTERMINATOR = '\t'
    ) WITH(
        trip_type TINYINT,
        trip_type_desc VARCHAR(15)
    )AS trip_type