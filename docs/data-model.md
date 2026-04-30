# Data Model

This document describes the main data layers, tables, and datasets used in the Divvy demand forecasting pipeline.

## S3 Layer Model

The project uses a layered S3 structure:

```text
s3://gbender-divvy-data-platform/

raw/
curated/
analytics/
ml/
```

Each layer has a separate purpose.

| Layer | Purpose |
|---|---|
| raw | Stores downloaded source files and extracted CSVs |
| curated | Stores cleaned trip-level Parquet data |
| analytics | Stores derived analytical datasets |
| ml | Stores model metrics, predictions, and related ML outputs |

## Raw Layer

The raw layer stores source data before transformation.

```text
raw/
  zip/
    year=YYYY/month=MM/
  csv/
    tripdata/
      year=YYYY/month=MM/
```

### Raw ZIP Files

Raw ZIP files are stored to preserve the original monthly download.

Example:

```text
raw/zip/year=2026/month=03/202603-divvy-tripdata.zip
```

### Raw CSV Files

Extracted CSV files are stored separately for Glue processing.

Example:

```text
raw/csv/tripdata/year=2026/month=03/202603-divvy-tripdata.csv
```

## Curated Layer

The curated layer stores cleaned trip-level records in Parquet format.

Expected table:

```text
divvy_db.tripdata_curated
```

The exact table name may vary depending on the deployed Glue job configuration, but the purpose is the same: cleaned ride-level Divvy records.

### Typical Curated Columns

```text
ride_id
rideable_type
started_at
ended_at
start_station_name
start_station_id
end_station_name
end_station_id
start_lat
start_lng
end_lat
end_lng
member_casual
year_num
month_num
```

### Curated Transformations

The curation job handles:

- Timestamp parsing
- Data type casting
- Invalid station filtering
- Partition assignment
- Parquet output
- Glue Catalog updates

## Analytics Layer

The analytics layer stores derived datasets used for reporting and modeling.

Main table:

```text
divvy_db.station_hour_departures
```

## Station-Hour Departures Table

This is the core modeling input table.

It aggregates cleaned ride-level records into hourly station-level departure counts.

### Grain

One row per:

```text
start_station_id
trip_date
trip_hour
```

### Typical Columns

```text
start_station_id
start_station_name
trip_date
trip_hour
station_hour_departures
year_num
month_num
```

Additional calendar columns may also exist depending on the Glue job output.

### Purpose

This table answers:

> How many rides departed from each station during each hour?

It is used as the base demand signal for model training.

## Weather Table

Weather data is stored in:

```text
divvy_db.weather_hourly
```

### Grain

One row per:

```text
trip_date
trip_hour
```

### Typical Columns

```text
trip_date
trip_hour
temperature_2m
relative_humidity_2m
precipitation
snowfall
weather_code
wind_speed_10m
wind_gusts_10m
year_num
month_num
```

### Purpose

Weather data is joined to the station-hour demand dataset by:

```text
trip_date
trip_hour
```

This gives every station-hour the same city-level weather context for that hour.

## Dense Modeling Dataset

The model does not train directly on only observed station-hour records.

The model job creates a dense grid:

```text
every station
× every date
× every hour from 0 to 23
```

Then it left joins observed station-hour departures.

Missing departures are filled with zero.

### Why This Matters

The station-hour aggregate only contains rows where rides occurred. Missing rows often represent true zero demand.

Without the dense grid, the model would be trained mainly on active demand periods and would not properly learn inactive station-hours.

## Modeling Target

Raw target column from the aggregate table:

```text
station_hour_departures
```

Model target column:

```text
departures
```

The model script casts the raw target to a double and aliases it as `departures`.

## Feature Dataset

The model job adds calendar, lag, rolling, and weather features.

### Calendar Features

```text
trip_hour
dow_num
month_num
is_weekend
```

### Station Feature

```text
station_idx
```

Generated from `start_station_id` using a Spark `StringIndexer`.

### Lag Features

```text
lag_24
lag_168
```

These represent:

- Previous day same-hour demand
- Previous week same-hour demand

### Rolling Features

```text
roll_mean_24
roll_mean_168
roll_std_168
```

These summarize recent demand patterns for each station.

### Weather Features

```text
temperature_2m
relative_humidity_2m
precipitation
snowfall
weather_code
wind_speed_10m
wind_gusts_10m
has_precipitation
has_snowfall
is_freezing
is_windy
```

## ML Metrics Table

Historical model metrics are stored in:

```text
divvy_db.model_metrics
```

S3 path:

```text
s3://gbender-divvy-data-platform/ml/model-metrics/
```

### Grain

One row per:

```text
run_id
model_name
```

Each model training run typically writes at least two rows:

```text
baseline
improved_model_with_weather
```

### Typical Columns

```text
run_id
run_ts_utc
run_date
run_mode
experiment_name
feature_set_name
model_family
weather_enabled
model_name
mae
rmse
wmape_pct
nz_mae
nz_rmse
nz_mape
train_start
valid_start
test_start
max_date
train_rows
valid_rows
test_rows
station_count
date_count
dense_rows
train_lookback_days
valid_days
test_days
```

## Latest Metrics Output

The latest run is also written as a convenience CSV:

```text
s3://gbender-divvy-data-platform/ml/model-metrics-latest/
```

This is not the historical metrics table. It is an overwrite-style convenience output for quickly viewing the latest run.

## Prediction Output

Latest prediction output is written to:

```text
s3://gbender-divvy-data-platform/ml/model-predictions-latest/
```

Predictions are useful for inspection, but metrics are the primary experiment tracking output.

## Data Quality Considerations

Important checks include:

- No missing weather rows for the modeling range
- No empty train, validation, or test datasets
- No null station IDs in the modeling dataset
- No null trip dates or trip hours
- Dense grid row count matches expected station × date × hour count
- Target values are non-null after zero-fill

## Future Data Model Improvements

Planned improvements:

- Add a model artifact table.
- Add a prediction history table partitioned by scoring date.
- Add a station dimension table.
- Add a calendar dimension table.
- Add holiday and event features.
- Add station activity tier features.
