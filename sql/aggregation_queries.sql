SELECT
    start_station_id,
    start_station_name,
    CAST(started_at AS date) AS trip_date,
    hour(started_at) AS trip_hour,
    day_of_week(started_at) AS day_of_week,
    COUNT(*) AS station_hour_departures,
    year(started_at) AS year_num,
    month(started_at) AS month_num
FROM divvy_db.curated_tripdata
WHERE start_station_id IS NOT NULL
  AND trim(start_station_id) <> ''
  AND start_station_name IS NOT NULL
  AND trim(start_station_name) <> ''
GROUP BY
    start_station_id,
    start_station_name,
    CAST(started_at AS date),
    hour(started_at),
    day_of_week(started_at),
    year(started_at),
    month(started_at)
ORDER BY trip_date, trip_hour;
