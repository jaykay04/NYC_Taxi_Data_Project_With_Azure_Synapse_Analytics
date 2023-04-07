USE nyc_taxi_discovery;

--Read json file using the JSON_VALUE function
SELECT  CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) payment_type,
        CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc') AS VARCHAR(15))payment_type_desc
    FROM OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0', --default
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a' --new line character for json, default
    )WITH(
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type;

--Read json file using the OPENJSON function
SELECT payment_type, description
    FROM OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        description VARCHAR(15) '$.payment_type_desc'
    );

--Read json file with an array using the JSON_VALUE function
SELECT CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) payment_type,
        CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc[0].value') AS VARCHAR(15)) payment_type_desc_0,
        CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc[1].value') AS VARCHAR(15)) payment_type_desc_1
    FROM OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type;

--Instead of having another column for the second occurence, we can use the OPENJSON function to include it as another record
--OPENJSON to explode data
SELECT payment_type, description
    FROM OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        payment_type_desc NVARCHAR(MAX) AS JSON
    )
    CROSS APPLY OPENJSON(payment_type_desc)
    WITH(
        sub_type SMALLINT,
        description VARCHAR(20) '$.value'
    );