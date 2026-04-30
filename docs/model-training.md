# Model Training

This document describes the current model training process for the Divvy demand forecasting pipeline.

## Objective

The model training job forecasts hourly station-level departures.

The main forecasting question is:

> To what extent can next-day hourly station departures be forecast from Divvy trip history data with enough accuracy to support proactive rebalancing decisions?

The current model is experimental. It improves over the baseline on several metrics, but it is not accurate enough for autonomous rebalancing decisions.

## Training Inputs

The model training job reads:

```text
divvy_db.station_hour_departures
divvy_db.weather_hourly
```

The station-hour table provides observed demand.

The weather table provides hourly weather features.

## Modeling Target

The source target column is:

```text
station_hour_departures
```

The model script aliases this to:

```text
departures
```

## Dense Grid Construction

The station-hour aggregate only contains observed station-hours. If a station had zero departures during an hour, that row may be missing.

The model job creates a dense grid:

```text
every station
× every date
× every hour from 0 to 23
```

Observed departures are left joined onto the grid.

Missing departure values are filled with zero.

This is necessary because the model must learn both zero-demand station-hours and active demand periods.

## Weather Join

Weather is joined by:

```text
trip_date
trip_hour
```

The job checks for missing weather rows after the join.

If weather is missing for the modeling range, the job fails instead of silently training with incomplete features.

## Feature Engineering

The current feature set includes calendar, station, lag, rolling, and weather features.

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

The model converts `start_station_id` into a numeric station index using Spark `StringIndexer`.

### Lag Features

```text
lag_24
lag_168
```

These represent previous demand for the same station:

- `lag_24`: previous day same hour
- `lag_168`: previous week same hour

### Rolling Features

```text
roll_mean_24
roll_mean_168
roll_std_168
```

These capture recent station-level demand trends.

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

## Train, Validation, and Test Split

The production split is time-based.

The current production configuration uses:

```text
test window: last 28 days
validation window: previous 28 days
training window: prior training lookback period
```

Time-based splitting is used instead of random splitting because this is a forecasting problem. Random splitting would leak future patterns into training.

## Run Modes

The model script supports two run modes.

### Dev Mode

```text
--run_mode dev
```

Dev mode is used for faster testing.

Typical dev behavior:

- Smaller date range
- Limited station count
- Faster iteration
- Useful for code changes and debugging

### Production Mode

```text
--run_mode prod
```

Production mode is used for benchmark runs.

Production is the default if no run mode is passed.

Recommended benchmark run:

```text
--run_mode prod
--write_metrics true
--write_predictions false
```

## Baseline Model

The baseline model uses historical average departures grouped by:

```text
start_station_id
trip_hour
dow_num
```

If a station-specific value is unavailable, the model falls back to an average by:

```text
trip_hour
dow_num
```

The baseline is simple, explainable, and useful for comparing whether the improved model adds value.

## Improved Model

The current improved model is a Spark ML random forest regressor.

Model family:

```text
RandomForestRegressor
```

Current model name:

```text
improved_model_with_weather
```

The improved model uses station, calendar, lag, rolling, and weather features.

## Metrics

The model job calculates:

```text
MAE
RMSE
WMAPE
Nonzero MAE
Nonzero RMSE
Nonzero MAPE
```

### Why Nonzero Metrics Matter

The dense station-hour grid contains many zero-demand rows.

Overall metrics can look better than operational performance because the model can do well on many inactive station-hours.

Nonzero metrics focus on station-hours where departures actually occurred. These are important for rebalancing decisions.

## Current Champion Benchmark

Current champion experiment:

```text
rf_weather_full_split
```

| Model | MAE | RMSE | WMAPE | Nonzero MAE | Nonzero RMSE | Nonzero MAPE |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 0.1221 | 0.5234 | 118.91% | 1.1869 | 1.8889 | 66.34% |
| Improved model with weather | 0.1151 | 0.4905 | 112.08% | 1.1894 | 1.9552 | 57.99% |

The improved model reduced:

```text
MAE: about 5.7%
RMSE: about 6.3%
WMAPE: about 5.7%
Nonzero MAPE: about 12.6%
```

The model improved several metrics but still has too much error for standalone operational use.

## Extended Training Experiment

A later experiment tested a longer training history.

Result:

- Overall MAE worsened slightly.
- Overall RMSE worsened slightly.
- WMAPE worsened slightly.
- Nonzero MAPE was nearly unchanged.

This suggests that simply adding more historical data does not necessarily improve this model. More recent history may be more predictive for this feature set and model design.

## Current Interpretation

The current model is a useful benchmark and prioritization signal.

It is not yet production-ready as an autonomous forecasting model.

The most accurate conclusion is:

```text
The pipeline is production-oriented, but the forecasting model remains experimental.
```

## Next Model Improvement

The next planned modeling improvement is a two-stage approach.

### Stage 1

Classify whether a station-hour will have zero or nonzero demand.

```text
departures > 0
```

### Stage 2

Predict the departure count for station-hours likely to have nonzero demand.

This approach may improve performance because the current single regressor is trying to learn both:

1. Whether demand exists.
2. How much demand occurs when it exists.

## Future Model Improvements

Planned improvements:

- Two-stage zero/nonzero and magnitude model
- Top-K station-hour ranking metrics
- Walk-forward monthly backtesting
- Holiday features
- Special event features
- Station activity tier features
- Saved model artifacts by run ID
- Separate training and scoring jobs
- Model monitoring over time
