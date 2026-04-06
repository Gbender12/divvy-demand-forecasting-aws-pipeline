# divvy-demand-forecasting-aws-pipeline

AWS-based ETL and forecasting workflow for next-day hourly Divvy bike station demand.

## Overview

This project documents an end-to-end AWS data engineering and forecasting workflow built to evaluate whether next-day hourly Divvy bike station departures can be forecast accurately enough to support proactive rebalancing decisions.

The project combines cloud-based ingestion, transformation, validation, and modeling steps using AWS services and PySpark. It is being organized as a portfolio artifact to showcase both data engineering and forecasting work.

## Problem Statement

Divvy demand varies by station, hour, and season. That makes proactive bike rebalancing difficult. This project tests whether historical trip data contains enough signal to support useful next-day station-level demand forecasts.

## Research Question

To what extent can next-day hourly station departures be forecast from Divvy trip history data with sufficient accuracy to support proactive rebalancing decisions?

## Pipeline Overview

![Pipeline flow](docs/pipeline-flow.png)

At a high level, the workflow is:

1. Ingest public Divvy trip data into Amazon S3.
2. Catalog the raw data with AWS Glue.
3. Clean and standardize trip-level data into a curated table.
4. Aggregate station-level hourly departures for forecasting.
5. Build a dense station-date-hour grid so zero-demand hours are represented.
6. Evaluate baseline and improved forecasting approaches.

## Tech Stack

- AWS S3
- AWS Glue
- AWS Athena
- PySpark
- Python
- Jupyter / AWS Glue Studio Notebook

## Repository Structure

```text
docs/
  pipeline-flow.png
  screenshots/
    athena-validation-aggregate-output.png
    athena-validation-cleaned-trip-table.png
    athena-validation-duplicate-check.png
    athena-validation-raw-timestamp-fix.png
    glue-crawler.png
    glue-job-aggregate.png
    glue-job-clean-run-history.png
    notebook-results-model-metrics.png
    s3-layout.png

sql/
  aggregation_queries.sql
  athena_validation_queries.sql
  qa_checks.sql

.gitignore
LICENSE
README.md
