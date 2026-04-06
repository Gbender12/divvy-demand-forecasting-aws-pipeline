-- Athena validation queries for the Divvy AWS pipeline

-- 1. Preview cleaned trip-level table
SELECT
    ride_id,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    member_casual,
    year_num,
    month_num
FROM divvy_db.divvy_clean
WHERE started_at IS NOT NULL
  AND ended_at IS NOT NULL
LIMIT 20;

-- 2. Preview hourly station departures aggregate
SELECT
    start_station_id,
    start_station_name,
    trip_date,
    trip_hour,
    day_of_week,
    station_hour_departures,
    year_num,
    month_num
FROM divvy_db.station_hour_departures
ORDER BY trip_date DESC, trip_hour DESC
LIMIT 20;

-- 3. Validate raw timestamp cleanup logic
SELECT
    started_at AS raw_started_at,
    trim(both '"' from started_at) AS trimmed_started_at,
    CAST(trim(both '"' from started_at) AS timestamp) AS cleaned_started_at,
    ended_at AS raw_ended_at,
    trim(both '"' from ended_at) AS trimmed_ended_at,
    CAST(trim(both '"' from ended_at) AS timestamp) AS cleaned_ended_at
FROM divvy_db.raw
WHERE started_at IS NOT NULL
  AND ended_at IS NOT NULL
LIMIT 20;
