USE nyc_taxi_discovery;

-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'calendar.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        HEADER_ROW = TRUE,
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS cal

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'calendar.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        HEADER_ROW = TRUE,
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(

    )AS cal

--Check the data types
EXEC sp_describe_first_result_set N'SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''calendar.csv'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        HEADER_ROW = TRUE,
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''\n''
    ) 
    AS cal'

SELECT
    *
FROM
    OPENROWSET(
        BULK 'calendar.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        HEADER_ROW = TRUE,
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        date_key INT,
        date   DATE,
        year SMALLINT,
        month TINYINT,
        day    TINYINT,
        day_name VARCHAR(10),
        day_of_year SMALLINT,
        week_of_month TINYINT,
        week_of_year TINYINT,
        month_name VARCHAR(10),
        year_month INT,
        year_week INT
    )AS cal
