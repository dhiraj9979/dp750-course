-- M1-C6: paste this into the pipeline's SQL transformation file
-- Pipeline default catalog: dbw_dp750_course | default schema: m1_declarative

-- Bronze: ingest new files from the landing volume, each file processed once
CREATE OR REFRESH STREAMING TABLE bronze_trips
COMMENT 'Raw taxi CSV files from the landing volume'
AS SELECT * FROM STREAM read_files(
  '/Volumes/dbw_dp750_course/dp750/landing/taxi/',
  format => 'csv',
  header => true
);

-- Silver: typed and validated trips
CREATE OR REFRESH MATERIALIZED VIEW silver_trips (
  CONSTRAINT valid_fare     EXPECT (fare_amount > 0)   ON VIOLATION DROP ROW,
  CONSTRAINT valid_distance EXPECT (trip_distance > 0)
)
COMMENT 'Typed and validated trips'
AS SELECT
  CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
  CAST(trip_distance AS DOUBLE)           AS trip_distance,
  CAST(fare_amount AS DOUBLE)             AS fare_amount,
  CAST(pickup_zip AS INT)                 AS pickup_zip
FROM bronze_trips;

-- Gold: average fare by pickup zip
CREATE OR REFRESH MATERIALIZED VIEW gold_fares_by_zip
COMMENT 'Average fare by pickup zip'
AS SELECT pickup_zip, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
FROM silver_trips
GROUP BY pickup_zip;
