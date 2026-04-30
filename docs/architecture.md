# Architecture Overview

This project uses AWS to build a production-oriented data engineering and machine learning pipeline for Divvy station-level demand forecasting.

The pipeline ingests public monthly Divvy trip files, stores raw and curated data in S3, enriches station-hour demand with weather data, trains forecasting models, and tracks model metrics in Athena.

## Architecture Goals

The architecture is designed around five goals:

1. Ingest public monthly Divvy files automatically.
2. Store data in clear S3 layers: raw, curated, analytics, and ML.
3. Use Glue and Athena for scalable data processing and validation.
4. Orchestrate the workflow with Step Functions.
5. Track model performance over time for repeatable experimentation.

## High-Level Flow

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

## AWS Services

| Service | Purpose |
|---|---|
| Amazon S3 | Stores raw, curated, analytics, and ML data |
| AWS Lambda | Resolves processing ranges and ingests raw monthly files |
| AWS Glue | Runs ETL, aggregation, weather refresh, and model training |
| AWS Glue Data Catalog | Stores table metadata for Athena and Glue |
| AWS Glue Crawler | Registers raw CSV partitions |
| Amazon Athena | Queries curated data, analytics tables, and model metrics |
| AWS Step Functions | Orchestrates the end-to-end workflow |
| Amazon EventBridge Scheduler | Triggers the pipeline on a monthly schedule |
| CloudWatch Logs | Stores Lambda, Glue, and Step Functions logs |

## S3 Layer Design

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

## Layer Responsibilities

### Raw Layer

The raw layer stores source data as close to the original form as practical.

```text
raw/
  zip/
    year=YYYY/month=MM/
  csv/
    tripdata/
      year=YYYY/month=MM/
```

This layer keeps both the downloaded ZIP files and the extracted CSV files. Keeping both makes the ingestion process easier to audit and rerun.

### Curated Layer

The curated layer contains cleaned trip-level records in Parquet format.

Typical curation work includes:

- Standardizing timestamps
- Casting numeric fields
- Removing invalid station records
- Preserving useful ride-level fields
- Partitioning by year and month

### Analytics Layer

The analytics layer contains derived analytical datasets.

The main analytics table is:

```text
divvy_db.station_hour_departures
```

This table contains station-hour departure counts and is the main input to model training.

### ML Layer

The ML layer contains model outputs and experiment tracking artifacts.

```text
ml/
  model-metrics/
  model-metrics-latest/
  model-predictions-latest/
```

The historical metrics table is queryable through Athena:

```text
divvy_db.model_metrics
```

## Data Flow Details

### 1. Monthly Schedule

EventBridge Scheduler starts the Step Functions state machine during a monthly window.

Current schedule pattern:

```text
cron(0 8 3-10 * ? *)
```

Timezone:

```text
America/Chicago
```

This runs at 8:00 AM from the 3rd through the 10th day of each month. The window gives the pipeline several chances to detect when the prior month’s Divvy file becomes available.

### 2. Date Range Resolution

The `ResolveDateRange` Lambda determines the target months for each stage.

The pipeline accounts for month-boundary overlap because monthly Divvy files can contain rides whose timestamps fall into adjacent months. For example, a March source file may contain rides that started in February or ended in April.

### 3. Raw Ingestion

The raw ingest Lambda downloads monthly Divvy ZIP files, stores the ZIP, extracts the main CSV, and stores the CSV in the raw S3 layer.

The Lambda returns whether new data was loaded. Step Functions can stop early if no new raw file is available and downstream processing is not forced.

### 4. Raw Cataloging

A Glue crawler registers new raw CSV partitions.

This is used because the raw CSV files are written by Lambda rather than by a Glue job with built-in catalog updates.

### 5. Curated and Weather Jobs

Tripdata curation and weather refresh can run in parallel because they do not depend on each other.

The tripdata job prepares cleaned trip-level data.

The weather job prepares hourly weather features.

### 6. Aggregation

The aggregation job waits until both curation and weather refresh are complete.

It creates station-hour departure counts from curated trip records.

### 7. Model Training

The model job reads the station-hour demand table, joins weather, builds a dense station-date-hour grid, creates lag and rolling features, trains models, evaluates them, and writes metrics.

## Current Architecture Status

Implemented:

- Monthly scheduled orchestration
- Raw ZIP and CSV ingestion
- S3 layered structure
- Glue Catalog integration
- Glue ETL jobs
- Weather enrichment
- Station-hour aggregation
- Model training and metrics tracking
- Athena validation

Remaining architecture improvements:

- Infrastructure as code
- Saved model artifacts by run ID
- Separate training and scoring workflows
- Model monitoring
- Data quality reporting
- Alerting on failed pipeline runs
