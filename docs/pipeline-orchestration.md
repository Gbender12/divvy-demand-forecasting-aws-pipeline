# Pipeline Orchestration

This document describes how the Divvy demand forecasting pipeline is orchestrated using EventBridge Scheduler, AWS Step Functions, Lambda, Glue, and Athena-queryable outputs.

## Orchestration Goals

The orchestration layer is designed to:

1. Run the pipeline automatically on a monthly schedule.
2. Detect whether new monthly Divvy data is available.
3. Avoid unnecessary downstream processing when no new raw data exists.
4. Handle month-boundary overlap in source files.
5. Run independent jobs in parallel where possible.
6. Track model outputs and metrics after training.

## State Machine Overview

```text
ResolveDateRange
  -> RawMonthlyIngest
  -> Choice: Continue?
  -> Run raw crawler
  -> Parallel:
       - Tripdata curation
       - Weather hourly refresh
  -> Station-hour demand aggregation
  -> Model training
```

## EventBridge Schedule

The state machine is triggered by EventBridge Scheduler.

Current schedule pattern:

```text
cron(0 8 3-10 * ? *)
```

Timezone:

```text
America/Chicago
```

This runs at 8:00 AM from the 3rd through the 10th day of each month.

This is intentional. Monthly Divvy files are not guaranteed to be available immediately on the first day of the month, so the schedule gives the pipeline a controlled window to check for the prior month’s file.

## Scheduler Payload

The EventBridge Scheduler payload can be empty:

```json
{}
```

When no date range is passed, `ResolveDateRange` can default to the expected production behavior, such as processing the previous month.

Manual runs can pass explicit dates:

```json
{
  "target_start_yyyymm": "202603",
  "target_end_yyyymm": "202603"
}
```

Manual reruns can also force downstream processing:

```json
{
  "target_start_yyyymm": "202603",
  "target_end_yyyymm": "202603",
  "force_downstream": true
}
```

## ResolveDateRange Lambda

The first Lambda calculates the month ranges used by the pipeline.

It returns separate ranges for:

- Raw ingestion
- Tripdata curation
- Weather refresh
- Aggregation
- Model training

## Overlap Policy

Monthly Divvy files can include trip records whose timestamps cross month boundaries.

Example:

- March source file may include trips that started in February.
- March source file may include trips that ended in April.

The current approach widens raw ingestion and tripdata curation by one previous month.

This helps ensure that overlapping records are available when rebuilding downstream outputs.

## Raw Ingestion Lambda

The raw ingestion Lambda downloads monthly Divvy ZIP files and extracts the main CSV.

Outputs:

```text
raw/zip/year=YYYY/month=MM/
raw/csv/tripdata/year=YYYY/month=MM/
```

The Lambda returns status details for each requested month.

Example statuses:

```text
loaded
already_loaded
source_unavailable
failed
```

It also returns summary fields such as:

```text
any_new_data_loaded
all_sources_unavailable
all_already_loaded
```

## Continue Choice

After raw ingestion, the Step Function decides whether to continue.

The normal behavior is:

- Continue if new raw data was loaded.
- Stop if all requested files already exist and no force flag was passed.
- Continue if `force_downstream = true`.

This prevents repeated scheduled runs from unnecessarily triggering Glue jobs after the monthly file has already been processed.

## Raw Crawler

The raw crawler runs after new raw data is loaded.

It updates raw table partitions in the Glue Catalog.

This is needed because the raw CSVs are written directly by Lambda.

## Parallel Glue Jobs

After raw cataloging, two Glue jobs can run in parallel:

```text
Tripdata curation
Weather hourly refresh
```

These are independent. The curation job processes trip records. The weather job processes hourly weather data.

Running these in parallel reduces total pipeline runtime.

## Aggregation Job

The aggregation job runs after both parallel branches finish.

It builds:

```text
divvy_db.station_hour_departures
```

This table contains station-hour departure counts and supports the model training job.

## Model Training Job

The model training job runs after aggregation.

It performs:

- Dense station-date-hour grid creation
- Weather join
- Feature engineering
- Baseline model evaluation
- Improved model training
- Metrics calculation
- Metrics writing to S3 and Athena

Historical model metrics are stored under:

```text
s3://gbender-divvy-data-platform/ml/model-metrics/
```

and queried through:

```text
divvy_db.model_metrics
```

## Development and Production Runs

The model script supports two modes.

### Dev Mode

```text
--run_mode dev
```

Used for fast testing.

Typical dev behavior:

- Smaller date range
- Limited station count
- Faster iteration
- Optional metrics writing

### Production Mode

```text
--run_mode prod
```

Production is also the default if no run mode is passed.

Typical production behavior:

- Full available modeling dataset
- All stations
- Metrics written to `model_metrics`
- Predictions written only when needed

Recommended production benchmark run:

```text
--run_mode prod
--write_metrics true
--write_predictions false
```

## Failure Handling

Pipeline failure handling currently relies on:

- Lambda return statuses
- Step Functions state transitions
- Glue job failures
- CloudWatch logs
- Data validation checks inside Glue jobs

Important validation examples:

- Missing weather rows fail the model job.
- Empty train, validation, or test sets fail the model job.
- Missing target columns fail the model job.

## Current Orchestration Strengths

Implemented:

- Monthly scheduling
- Date range resolution
- Conditional downstream execution
- Parallel job execution
- Raw crawler integration
- Glue ETL orchestration
- Model metrics tracking

## Future Orchestration Improvements

Planned improvements:

- Add SNS or email alerts for failures.
- Add dead-letter queue handling where appropriate.
- Add infrastructure as code.
- Separate model training from scoring.
- Add a daily or monthly scoring job.
- Save model artifacts by run ID.
- Add formal data quality reporting.
