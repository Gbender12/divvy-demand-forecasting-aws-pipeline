# Experiment Tracking

This document describes how model experiments are tracked in the Divvy demand forecasting pipeline.

## Purpose

Experiment tracking is used to compare model versions, feature sets, run modes, and training windows.

The goal is to avoid relying on one-off notebook output. Every meaningful model run should produce queryable metrics that can be compared over time.

## Metrics Table

Historical model metrics are stored in:

```text
divvy_db.model_metrics
```

S3 path:

```text
s3://gbender-divvy-data-platform/ml/model-metrics/
```

The table is queryable from Athena.

## Latest Metrics Output

The latest run is also written to:

```text
s3://gbender-divvy-data-platform/ml/model-metrics-latest/
```

This is a convenience output. It is not the historical table.

The historical source of truth is:

```text
divvy_db.model_metrics
```

## Table Grain

The metrics table has one row per model per run.

A typical training run writes two rows:

```text
baseline
improved_model_with_weather
```

Future experiments may add additional rows, such as:

```text
two_stage_hurdle_with_weather
gbt_with_weather
```

## Important Columns

### Run Metadata

```text
run_id
run_ts_utc
run_date
run_mode
experiment_name
feature_set_name
model_family
weather_enabled
```

### Split Metadata

```text
train_start
valid_start
test_start
max_date
train_rows
valid_rows
test_rows
```

### Dataset Metadata

```text
station_count
date_count
dense_rows
```

### Metric Columns

```text
mae
rmse
wmape_pct
nz_mae
nz_rmse
nz_mape
```

### Optional Configuration Columns

```text
train_lookback_days
valid_days
test_days
```

Some older records may have null values for newer metadata fields because the schema evolved over time.

## Run Modes

### Dev Runs

Dev runs are used for fast testing.

```text
--run_mode dev
```

Dev records may be written to the metrics table if `--write_metrics true`.

When analyzing official performance, filter them out:

```sql
WHERE run_mode = 'prod'
```

### Production Runs

Production runs use the full modeling dataset and are used for official benchmark comparisons.

```text
--run_mode prod
```

Production is the default if no run mode is passed.

## Current Experiment Names

### rf_weather_full_split

Current champion experiment.

This experiment uses:

- Baseline historical average
- Random forest regressor
- Lag and rolling features
- Weather features
- Production time-based split

### rf_weather_extended_train

Experiment that tested a longer training history.

This did not improve the model meaningfully compared with the current champion.

## Common Athena Queries

### View Latest Metrics

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
ORDER BY run_ts_utc DESC, model_name;
```

### View Production Runs Only

```sql
SELECT
    run_ts_utc,
    experiment_name,
    model_name,
    ROUND(mae, 4) AS mae,
    ROUND(rmse, 4) AS rmse,
    ROUND(wmape_pct, 2) AS wmape_pct,
    ROUND(nz_mae, 4) AS nz_mae,
    ROUND(nz_rmse, 4) AS nz_rmse,
    ROUND(nz_mape, 2) AS nz_mape,
    train_start,
    valid_start,
    test_start
FROM divvy_db.model_metrics
WHERE run_mode = 'prod'
ORDER BY run_ts_utc DESC, model_name;
```

### Compare Experiments

```sql
SELECT
    experiment_name,
    model_name,
    run_ts_utc,
    ROUND(mae, 4) AS mae,
    ROUND(rmse, 4) AS rmse,
    ROUND(wmape_pct, 2) AS wmape_pct,
    ROUND(nz_mape, 2) AS nz_mape
FROM divvy_db.model_metrics
WHERE run_mode = 'prod'
ORDER BY experiment_name, run_ts_utc DESC, model_name;
```

### Show Source Files

```sql
SELECT
    "$path" AS source_file,
    run_ts_utc,
    run_mode,
    experiment_name,
    model_name,
    mae,
    rmse,
    wmape_pct,
    nz_mape
FROM divvy_db.model_metrics
ORDER BY run_ts_utc DESC, model_name;
```

## Current Champion Metrics

Current champion experiment:

```text
rf_weather_full_split
```

| Model | MAE | RMSE | WMAPE | Nonzero MAE | Nonzero RMSE | Nonzero MAPE |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 0.1221 | 0.5234 | 118.91% | 1.1869 | 1.8889 | 66.34% |
| Improved model with weather | 0.1151 | 0.4905 | 112.08% | 1.1894 | 1.9552 | 57.99% |

The improved model performed better on:

- MAE
- RMSE
- WMAPE
- Nonzero MAPE

It performed slightly worse on:

- Nonzero MAE
- Nonzero RMSE

## Extended Training Experiment

The extended training experiment used more historical data.

It did not improve the champion model.

Compared with the current champion, the extended training version:

- Slightly worsened MAE
- Slightly worsened RMSE
- Slightly worsened WMAPE
- Left nonzero MAPE nearly unchanged

This suggests that more history is not automatically better for this problem.

## Interpreting Metrics

### MAE

Mean absolute error across the full dense station-hour grid.

This is influenced by the large number of zero-demand rows.

### RMSE

Root mean squared error.

This penalizes larger errors more than MAE.

### WMAPE

Weighted mean absolute percentage error.

This is more stable than regular MAPE on sparse data because it uses total actual demand in the denominator.

### Nonzero MAPE

MAPE calculated only where actual departures are greater than zero.

This is useful because the dense grid contains many zero-demand rows.

## Recommended Experiment Workflow

1. Run changes in dev mode.
2. Inspect notebook output.
3. Run production with metrics enabled.
4. Query `divvy_db.model_metrics`.
5. Compare against the current champion.
6. Promote the new model only if it improves meaningful production metrics.

Recommended production command:

```text
--run_mode prod
--write_metrics true
--write_predictions false
```

## Future Tracking Improvements

Planned improvements:

- Save model artifacts by run ID.
- Save full model configuration JSON by run ID.
- Save feature list by run ID.
- Save Git commit hash with each run.
- Add model version aliases such as champion and challenger.
- Add scoring metrics after actual future data becomes available.
- Add top-K operational ranking metrics.
