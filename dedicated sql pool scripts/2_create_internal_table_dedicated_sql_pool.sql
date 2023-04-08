CREATE SCHEMA dwh
GO

CREATE TABLE dwh.trip_data_green_agg
WITH(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = ROUND_ROBIN
)
AS SELECT * FROM staging.ext_trip_data_green_agg 
GO

SELECT TOP 100 * FROM dwh.trip_data_green_agg;