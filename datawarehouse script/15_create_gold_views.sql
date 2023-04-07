USE nyc_taxi_ldw
GO

--create view for trip_data_green
DROP VIEW IF EXISTS gold.vw_trip_data_green
GO

CREATE VIEW gold.vw_trip_data_green
AS
SELECT  
    trip_data_green.filepath(1) AS year,
    trip_data_green.filepath(2) AS month,
    trip_data_green.*
FROM OPENROWSET(
    BULK 'gold/trip_data_green/year=*/month=*/*.parquet',
    DATA_SOURCE = 'nyc_taxi_src',
    FORMAT = 'PARQUET'
) 
WITH(
       borough              VARCHAR(15),
       trip_date            DATE,
       trip_day             VARCHAR(10),
       trip_day_wknd_ind    CHAR(1),
       card_trip_count      INT,
       cash_trip_count      INT
        )
AS trip_data_green
GO

SELECT TOP 100 * FROM gold.vw_trip_data_green
GO