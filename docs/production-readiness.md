# Production Readiness

This document summarizes the production-readiness status of the Divvy demand forecasting project.

The project should be evaluated in two parts:

1. Pipeline production readiness
2. Model production readiness

The data pipeline is production-oriented. The forecasting model is still experimental.

## Current Summary

```text
Pipeline: production-oriented, but not fully hardened
Model: experimental, not ready for autonomous decisions
```

## What Is Already Production-Oriented

### Automated Ingestion

The pipeline can ingest monthly Divvy files automatically.

Implemented:

- EventBridge monthly schedule
- Date range resolution Lambda
- Raw monthly ingest Lambda
- ZIP and CSV storage in S3
- Source availability detection

### S3 Layering

The project uses clear S3 layers:

```text
raw/
curated/
analytics/
ml/
```

This makes the data flow easier to understand, audit, and extend.

### Cataloged Tables

Glue Catalog tables make the data queryable through Athena.

Important tables include:

```text
divvy_db.station_hour_departures
divvy_db.weather_hourly
divvy_db.model_metrics
```

### Glue ETL Jobs

Glue jobs handle:

- Tripdata curation
- Weather refresh
- Station-hour aggregation
- Model training and evaluation

### Orchestration

Step Functions coordinates the full pipeline.

The workflow includes:

- Lambda execution
- Conditional continuation
- Raw crawler execution
- Parallel Glue jobs
- Downstream aggregation
- Model training

### Experiment Tracking

Model metrics are written to an Athena-queryable table:

```text
divvy_db.model_metrics
```

This allows experiments to be compared over time.

### Dev and Production Modes

The model script supports:

```text
--run_mode dev
--run_mode prod
```

This makes development safer and faster.

## Current Model Status

The current champion experiment is:

```text
rf_weather_full_split
```

Current benchmark:

| Model | MAE | RMSE | WMAPE | Nonzero MAE | Nonzero RMSE | Nonzero MAPE |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 0.1221 | 0.5234 | 118.91% | 1.1869 | 1.8889 | 66.34% |
| Improved model with weather | 0.1151 | 0.4905 | 112.08% | 1.1894 | 1.9552 | 57.99% |

The improved model beats the baseline on several metrics, including MAE, RMSE, WMAPE, and nonzero MAPE.

However, the error is still too high for autonomous rebalancing decisions.

## Why the Model Is Not Fully Production Ready

The model is not ready for autonomous decisions because:

- Nonzero MAPE remains high.
- The dense grid is highly zero-inflated.
- The model does not yet separate zero-demand prediction from demand magnitude prediction.
- The model has not been validated with walk-forward monthly backtesting.
- The model does not yet produce operational ranking metrics.
- There is no separate scoring job for next-day predictions.
- Trained model artifacts are not yet versioned by run ID.

## Production Use Case Distinction

The current model may be useful as:

```text
A prioritization signal
```

It is not ready as:

```text
A fully automated rebalancing decision system
```

A practical production system would likely show operators a ranked list of station-hours that may need attention, rather than directly dispatching trucks.

## Remaining Pipeline Hardening

### Infrastructure as Code

Current AWS resources should eventually be represented in infrastructure as code.

Options:

```text
Terraform
AWS CDK
CloudFormation
```

This would make the environment easier to recreate.

### Alerting

Add alerts for:

- Failed Lambda executions
- Failed Glue jobs
- Failed Step Functions executions
- Missing source files after the monthly schedule window
- Missing weather data
- Empty model training datasets

### Data Quality Checks

Recommended checks:

- No duplicate ride IDs in curated data
- No null station IDs in modeling data
- No missing weather rows for the modeling range
- Dense grid row count matches expected count
- Train, validation, and test sets are non-empty
- Departure counts are non-negative
- Station count is within expected range

### Observability

Useful operational metrics:

- Pipeline runtime
- Number of rows processed by stage
- Number of source files loaded
- Number of curated rows written
- Number of station-hour rows generated
- Model training runtime
- Model metric changes over time

## Remaining Model Improvements

### Two-Stage Sparse Demand Model

The next planned modeling improvement is a two-stage model.

Stage 1:

```text
Predict whether departures > 0
```

Stage 2:

```text
Predict departure count for likely nonzero station-hours
```

This better matches the structure of the demand problem.

### Ranking Metrics

For rebalancing, exact demand forecasting may be less important than ranking the highest-risk station-hours.

Recommended metrics:

```text
Precision at K
Recall at K
Top-K high-demand capture
High-demand recall
False positive rate for quiet stations
Underprediction rate for active station-hours
```

### Walk-Forward Backtesting

A single train/test split is not enough for production confidence.

Future testing should evaluate multiple monthly windows.

Example:

```text
Train through August, test September
Train through September, test October
Train through October, test November
```

This would show whether the model is stable over time.

### Model Artifact Versioning

Future training runs should save:

```text
trained model
feature list
hyperparameters
training date range
evaluation metrics
run ID
Git commit hash
```

Suggested path:

```text
s3://gbender-divvy-data-platform/ml/models/run_id=<run_id>/
```

### Separate Training and Scoring

Current model training is focused on evaluation.

A production system should separate:

```text
Training job
Scoring job
```

Training job:

- Trains model
- Evaluates model
- Saves model artifact
- Writes metrics

Scoring job:

- Loads approved model
- Builds next-day features
- Writes next-day station-hour predictions

## Suggested Production Architecture Roadmap

### Phase 1: Current State

Implemented:

- Automated ingestion
- ETL
- Aggregation
- Weather enrichment
- Model training
- Metrics tracking

### Phase 2: Model Quality Improvements

Next:

- Two-stage model
- Ranking metrics
- Walk-forward backtesting
- Better calendar and station features

### Phase 3: Operational ML

Next:

- Save model artifacts
- Separate training and scoring
- Create next-day prediction table
- Add model monitoring

### Phase 4: Production Hardening

Next:

- Infrastructure as code
- Alerting
- Data quality dashboard
- Cost monitoring
- Access control review

## Current Production Readiness Assessment

| Area | Status |
|---|---|
| S3 data layout | Implemented |
| Raw ingestion | Implemented |
| ETL jobs | Implemented |
| Weather enrichment | Implemented |
| Aggregation | Implemented |
| Orchestration | Implemented |
| Scheduling | Implemented |
| Metrics tracking | Implemented |
| Model artifact versioning | Not implemented |
| Separate scoring job | Not implemented |
| Model monitoring | Not implemented |
| Infrastructure as code | Not implemented |
| Autonomous decision readiness | Not ready |

## Conclusion

This project has a production-oriented data pipeline but not yet a production-ready forecasting model.

The strongest current positioning is:

```text
A production-oriented AWS data engineering and ML pipeline for Divvy demand forecasting, with an experimental model and a clear roadmap toward operational readiness.
```

That framing is honest and stronger than claiming the model is fully production ready before the error profile supports that conclusion.
