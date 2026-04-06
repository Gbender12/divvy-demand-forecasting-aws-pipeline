# divvy-demand-forecasting-aws-pipeline
AWS-based ETL and forecasting workflow for next-day hourly bike station demand

## Overview
This project builds an AWS-based data engineering and forecasting pipeline to evaluate whether next-day hourly Divvy bike station departures can be forecast accurately enough to support proactive rebalancing decisions.

## Problem Statement
Divvy demand varies by station, hour, and season. That makes proactive bike rebalancing difficult. This project tests whether historical trip data can support useful next-day station-level demand forecasts.

## Tech Stack
- AWS S3
- AWS Glue
- AWS Athena
- PySpark
- Python
- Jupyter / Glue Studio Notebook

## Project Components
- Raw trip data ingestion to S3
- Glue crawler and catalog registration
- Cleaning and timestamp normalization
- Deduplication and hourly station aggregation
- Dense station-date-hour grid creation
- Baseline and improved forecasting models
- Evaluation using MAE, RMSE, WMAPE, and non-zero MAPE

## Current Status
The pipeline is working end to end. Baseline and improved models were evaluated, and the improved model reduced error versus baseline, but results still fell short of the target needed for stand-alone operational use.

## Repository Structure
TBD

## Next Steps
- Finalize repo organization
- Add architecture diagrams
- Add cleaned ETL scripts
- Add SQL validation queries
- Document production-readiness roadmap
