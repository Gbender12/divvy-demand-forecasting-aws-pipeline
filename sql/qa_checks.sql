-- Data quality checks for Divvy pipeline outputs

-- 1. Check for duplicate ride_ids in cleaned trip-level data
SELECT
    ride_id,
    COUNT(*) AS duplicate_count
FROM divvy_db.curated_tripdata
GROUP BY ride_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- 2. Check for blank or null start station identifiers
SELECT
    COUNT(*) AS bad_start_station_id_rows
FROM divvy_db.curated_tripdata
WHERE start_station_id IS NULL
   OR trim(start_station_id) = '';

-- 3. Check for blank or null start station names
SELECT
    COUNT(*) AS bad_start_station_name_rows
FROM divvy_db.curated_tripdata
WHERE start_station_name IS NULL
   OR trim(start_station_name) = '';

-- 4. Compare table row counts
SELECT 'raw' AS table_name, COUNT(*) AS row_count
FROM divvy_db.raw_tripdata
UNION ALL
SELECT 'divvy_clean' AS table_name, COUNT(*) AS row_count
FROM divvy_db.curated_tripdata
UNION ALL
SELECT 'station_hour_departures' AS table_name, COUNT(*) AS row_count
FROM divvy_db.station_hour_departures;

-- 5. Check aggregate output for blank station identifiers
SELECT
    COUNT(*) AS bad_aggregate_station_rows
FROM divvy_db.station_hour_departures
WHERE start_station_id IS NULL
   OR trim(start_station_id) = ''
   OR start_station_name IS NULL
   OR trim(start_station_name) = '';
