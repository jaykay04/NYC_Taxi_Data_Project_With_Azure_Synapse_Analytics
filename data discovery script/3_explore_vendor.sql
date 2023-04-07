USE nyc_taxi_discovery;

SELECT 
    *
FROM OPENROWSET(
    BULK 'vendor_unquoted.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'

) AS vendor

SELECT 
    *
FROM OPENROWSET(
    BULK 'vendor_escaped.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    ESCAPECHAR = '\'

) AS vendor

--If it has a quote, we can specify fieldquote parameter
SELECT 
    *
FROM OPENROWSET(
    BULK 'vendor.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIELDQUOTE = '"'  --the default fieldquote is double quotes

) AS vendor