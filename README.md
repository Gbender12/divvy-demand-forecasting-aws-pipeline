# Divvy Demand Forecasting AWS Pipeline

An AWS-based data engineering and machine learning pipeline for forecasting hourly Divvy bike demand at the station level.

This project ingests public Divvy trip data, curates it into analytics-ready Parquet tables, enriches it with hourly weather data, aggregates station-hour demand, trains forecasting models, and tracks model performance in Athena.

The project began as a capstone-style analysis and is being expanded into a production-oriented portfolio project focused on cloud data engineering, orchestration, repeatability, and model evaluation.

## Project Goal

Bike-share demand changes by station, hour, weekday, season, and weather. Poor demand visibility makes proactive bike rebalancing harder.

The main forecasting question is:

> To what extent can next-day hourly station departures be forecast from Divvy trip history data with enough accuracy to support proactive rebalancing decisions?

The current model is not production-ready for autonomous rebalancing decisions, but the pipeline demonstrates a production-style AWS workflow for ingestion, transformation, orchestration, feature engineering, model training, and experiment tracking.

## Current Status

The data pipeline is production-oriented and includes:

- Monthly raw Divvy file ingestion
- ZIP extraction and CSV storage in S3
- S3 raw, curated, analytics, and ML output layers
- Glue Catalog tables for Athena querying
- Glue ETL jobs for curation, weather refresh, aggregation, and model training
- Step Functions orchestration
- EventBridge scheduling
- Athena validation queries
- Model metrics tracking by run

The model is still experimental. It improves over the baseline on several metrics, but the remaining error is too high for fully automated operational use.

## Architecture

```text
EventBridge Scheduler
        |
        v
AWS Step Functions
        |
        v
ResolveDateRange Lambda
        |
        v
Raw Monthly Ingest Lambda
        |
        v
S3 raw ZIP and CSV storage
        |
        v
Glue Crawler
        |
        v
+-----------------------------+
| Parallel Glue jobs          |
|                             |
| 1. Tripdata curation        |
| 2. Weather hourly refresh   |
+-----------------------------+
        |
        v
Station-hour demand aggregation
        |
        v
Model training and evaluation
        |
        v
S3 ML outputs and Athena model_metrics table
```

## AWS Services Used

| Service | Purpose |
|---|---|
| Amazon S3 | Stores raw, curated, analytics, and ML output data |
| AWS Lambda | Resolves date ranges and ingests monthly Divvy files |
| AWS Glue | Runs ETL, aggregation, weather refresh, and model training jobs |
| AWS Glue Data Catalog | Stores table metadata for Athena and Glue |
| AWS Glue Crawler | Registers raw CSV partitions |
| Amazon Athena | Validates data and queries model metrics |
| AWS Step Functions | Orchestrates the pipeline |
| Amazon EventBridge Scheduler | Runs the pipeline on a monthly schedule |
| CloudWatch Logs | Captures Lambda, Glue, and orchestration logs |

## S3 Data Layout

```text
s3://gbender-divvy-data-platform/

raw/
  zip/
  csv/tripdata/

curated/
  tripdata/

analytics/
  station_hour_departures/

ml/
  model-metrics/
  model-metrics-latest/
  model-predictions-latest/
```

## Data Sources

### Divvy Trip Data

The primary dataset is public Divvy trip history data. Monthly files contain ride-level records, including ride ID, bike type, start and end timestamps, station information, coordinates, and rider type.

### Weather Data

Hourly weather data is added to support demand modeling. Weather features include temperature, humidity, precipitation, snowfall, weather code, wind speed, and wind gusts.

## Pipeline Summary

### 1. Date Range Resolution

The `ResolveDateRange` Lambda determines which months each pipeline stage should process.

The pipeline accounts for monthly file overlap. For example, a March Divvy file may contain rides that started in February or ended in April. Raw ingestion and tripdata curation use a one-month lookback so overlapping records are not missed.

### 2. Raw Ingestion

The raw ingest Lambda downloads monthly Divvy ZIP files, stores the ZIP files in S3, extracts the main CSV file, and stores the CSV in a partitioned raw S3 path.

The Lambda returns whether new data was loaded, allowing Step Functions to stop early if the source file is not available yet.

### 3. Tripdata Curation

The tripdata curation Glue job reads raw trip CSV data, cleans and standardizes fields, casts timestamps and numeric values, removes invalid records, and writes partitioned Parquet output.

### 4. Weather Refresh

The weather refresh Glue job loads hourly weather data and writes it to a cataloged Parquet table for later joins.

### 5. Station-Hour Aggregation

The aggregation Glue job builds the main analytics table:

```text
divvy_db.station_hour_departures
```

This table contains hourly station-level departure counts and is the main input to the model training job.

### 6. Model Training

The model training job builds a dense station-date-hour grid, joins observed departures, fills missing station-hours with zero demand, joins weather features, creates lag and rolling features, trains models, and writes metrics to Athena-queryable output.

## Dense Grid Methodology

The raw station-hour aggregate only contains station-hours where trips occurred. That creates a problem because missing station-hours often represent true zero demand.

To fix this, the model job creates a dense grid:

```text
every station
× every date
× every hour from 0 to 23
```

Observed departures are joined onto this grid, and missing values are filled with zero.

This allows the model to learn both inactive station-hours and active demand periods.

## Model Features

The current improved model uses:

- Station ID index
- Hour of day
- Day of week
- Month
- Weekend flag
- Previous day same-hour demand
- Previous week same-hour demand
- 24-hour rolling average
- 168-hour rolling average
- 168-hour rolling standard deviation
- Hourly weather features
- Weather condition flags

## Model Tracking

Model metrics are written to:

```text
divvy_db.model_metrics
```

Each run stores:

- Run timestamp
- Run mode
- Experiment name
- Model name
- Train, validation, and test date ranges
- Row counts
- MAE
- RMSE
- WMAPE
- Nonzero MAE
- Nonzero RMSE
- Nonzero MAPE

Example Athena query:

```sql
SELECT
    run_ts_utc,
    run_mode,
    experiment_name,
    model_name,
    mae,
    rmse,
    wmape_pct,
    nz_mae,
    nz_rmse,
    nz_mape,
    train_start,
    valid_start,
    test_start,
    train_rows,
    test_rows
FROM divvy_db.model_metrics
WHERE run_mode = 'prod'
ORDER BY run_ts_utc DESC, model_name;
```

## Current Production Benchmark

Current champion experiment:

```text
rf_weather_full_split
```

| Model | MAE | RMSE | WMAPE | Nonzero MAE | Nonzero RMSE | Nonzero MAPE |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 0.1221 | 0.5234 | 118.91% | 1.1869 | 1.8889 | 66.34% |
| Improved model with weather | 0.1151 | 0.4905 | 112.08% | 1.1894 | 1.9552 | 57.99% |

Compared with the baseline, the improved model reduced:

- MAE by about 5.7%
- RMSE by about 6.3%
- WMAPE by about 5.7%
- Nonzero MAPE by about 12.6%

The improved model performs better on several important metrics, but the forecast error is still too high for standalone rebalancing decisions.

## Recent Experiment: Extended Training History

An extended training experiment used more historical data, but it did not improve the model meaningfully.

The longer training window slightly worsened overall MAE, RMSE, and WMAPE while leaving nonzero MAPE nearly unchanged. This suggests that recent demand history is more useful than simply adding older records.

## Interpretation

The current model is useful as a benchmark and prioritization signal, but it is not ready for autonomous operational decisions.

The most accurate project conclusion is:

> The data pipeline is production-oriented, while the current forecasting model remains experimental. The model improves over the baseline in several areas, but additional sparse-demand modeling is needed before it can support production rebalancing decisions.

## Run Modes

The model training script supports two run modes.

### Dev Mode

Used for faster testing.

```text
--run_mode dev
```

Default dev behavior:

- Smaller date range
- Limited station count
- Faster model iteration
- Useful for code changes and debugging

### Production Mode

Used for full benchmark runs.

```text
--run_mode prod
```

Production is also the default if no run mode is passed.

Recommended production benchmark run:

```text
--run_mode prod
--write_metrics true
--write_predictions false
```

## EventBridge Schedule

The pipeline can be scheduled to run monthly through EventBridge Scheduler.

Current schedule pattern:

```text
cron(0 8 3-10 * ? *)
```

Timezone:

```text
America/Chicago
```

This runs at 8:00 AM from the 3rd through the 10th of each month, giving the source data time to become available.

## Repository Structure

```text
.
├── README.md
├── LICENSE
├── docs/
│   └── images/
├── glue/
│   ├── tripdata_curation.py
│   ├── weather_hourly_refresh.py
│   ├── station_hour_demand_aggregation.py
│   └── station_demand_model_train.py
├── lambda/
│   ├── resolve_date_range.py
│   └── divvy_raw_monthly_ingest.py
├── step-functions/
│   └── divvy-demand-pipeline.asl.json
├── sql/
│   ├── athena_validation_queries.sql
│   ├── qa_checks.sql
│   └── aggregation_queries.sql
├── config/
│   ├── sample_job_config.yaml
│   └── sample_s3_paths.yaml
└── results/
    ├── metrics_summary.csv
    ├── sample_predictions.csv
    └── feature_list.md
```

## Production Readiness

### Implemented

- Automated ingestion
- S3 data lake style layout
- Partitioned Parquet outputs
- Glue Catalog integration
- Weather enrichment
- Step Functions orchestration
- EventBridge scheduling
- Athena validation
- Experiment tracking
- Dev and production run modes

### Remaining Work

- Separate model training from daily scoring
- Save trained model artifacts by run ID
- Add next-day scoring output
- Add model monitoring
- Add ranking metrics for top station-hour demand
- Add walk-forward monthly backtesting
- Improve sparse-demand handling

## Next Improvements

The next planned modeling improvement is a two-stage approach:

1. Classify whether a station-hour will have zero or nonzero departures.
2. Predict departure count for likely nonzero station-hours.

This should better match the structure of the problem because the dense grid is heavily zero-inflated.

Additional future improvements include:

- Top-K station-hour ranking metrics
- Holiday and event features
- Station activity tier features
- Walk-forward backtesting
- Model artifact versioning
- Separate training and scoring jobs

## Key Lessons

- Monthly public data files can include overlapping records from adjacent months.
- A dense station-date-hour grid is necessary for sparse demand forecasting.
- More historical data does not always improve forecast performance.
- Weather features can improve some metrics but do not solve sparse demand by themselves.
- Production readiness requires orchestration, validation, repeatability, and monitoring, not just a trained model.

## Conclusion

This project demonstrates a production-oriented AWS pipeline for Divvy demand forecasting. It ingests raw monthly trip data, curates it, enriches it with weather, aggregates station-hour demand, trains forecasting models, and tracks performance over time.

The current model improves on the baseline in several areas, but it is not yet accurate enough for fully automated rebalancing decisions. The next phase will focus on sparse-demand modeling, ranking-based evaluation, model artifact versioning, and separating training from scoring.
