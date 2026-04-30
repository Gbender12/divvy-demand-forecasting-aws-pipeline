import sys
import pandas as pd
import uuid

from datetime import datetime, timezone, timedelta
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# Configuration
def get_optional_arg(name, default=None):
    flag = f"--{name}"
    if flag in sys.argv:
        return getResolvedOptions(sys.argv, [name])[name]
    return default


RUN_MODE = get_optional_arg("run_mode", "prod").lower()

if RUN_MODE == "dev":
    MODEL_START_DATE = get_optional_arg("model_start_date", "2026-03-01")
    MODEL_END_DATE = get_optional_arg("model_end_date", "2026-03-31")
    STATION_LIMIT_RAW = get_optional_arg("station_limit", "200")
    WRITE_PREDICTIONS = get_optional_arg("write_predictions", "false").lower() == "true"
    WRITE_METRICS = get_optional_arg("write_metrics", "true").lower() == "true"
else:
    MODEL_START_DATE = get_optional_arg("model_start_date", None)
    MODEL_END_DATE = get_optional_arg("model_end_date", None)
    STATION_LIMIT_RAW = get_optional_arg("station_limit", None)
    WRITE_PREDICTIONS = get_optional_arg("write_predictions", "true").lower() == "true"
    WRITE_METRICS = get_optional_arg("write_metrics", "true").lower() == "true"

STATION_LIMIT = int(STATION_LIMIT_RAW) if STATION_LIMIT_RAW else None

DATABASE_NAME = "divvy_db"
AGG_TABLE_NAME = "station_hour_departures"
WEATHER_TABLE_NAME = "weather_hourly"

RAW_TARGET_COL = "station_hour_departures"
MODEL_TARGET_COL = "departures"

METRICS_TABLE_NAME = "model_metrics"
METRICS_TABLE_PATH = "s3://gbender-divvy-data-platform/ml/model-metrics/"

LATEST_METRICS_OUTPUT_PATH = "s3://gbender-divvy-data-platform/ml/model-metrics-latest/"
PREDICTIONS_OUTPUT_PATH = "s3://gbender-divvy-data-platform/ml/model-predictions-latest/"

EXPERIMENT_NAME = "rf_weather_extended_train"
FEATURE_SET_NAME = "lags_rolls_weather"
MODEL_FAMILY = "random_forest_regressor"
WEATHER_ENABLED = True

if RUN_MODE == "dev":
    TRAIN_LOOKBACK_DAYS = int(get_optional_arg("train_lookback_days", "17"))
    VALID_DAYS = int(get_optional_arg("valid_days", "7"))
    TEST_DAYS = int(get_optional_arg("test_days", "7"))
else:
    TRAIN_LOOKBACK_DAYS = int(get_optional_arg("train_lookback_days", "390"))
    VALID_DAYS = int(get_optional_arg("valid_days", "28"))
    TEST_DAYS = int(get_optional_arg("test_days", "28"))
    
print("=== RUN CONFIGURATION ===")
print(f"RUN_MODE: {RUN_MODE}")
print(f"MODEL_START_DATE: {MODEL_START_DATE}")
print(f"MODEL_END_DATE: {MODEL_END_DATE}")
print(f"STATION_LIMIT: {STATION_LIMIT}")
print(f"WRITE_PREDICTIONS: {WRITE_PREDICTIONS}")
print(f"WRITE_METRICS: {WRITE_METRICS}")
# Load catalog tables
agg_df = spark.table(f"{DATABASE_NAME}.{AGG_TABLE_NAME}")
weather_raw_df = spark.table(f"{DATABASE_NAME}.{WEATHER_TABLE_NAME}")
# Main
if RAW_TARGET_COL not in agg_df.columns:
    raise ValueError(
        f"Expected raw target column '{RAW_TARGET_COL}' not found. "
        f"Available columns: {agg_df.columns}"
    )
# Prepare weather data
weather_feature_cols = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m"
]

weather_df = (
    weather_raw_df
    .select(
        F.to_date("trip_date").alias("trip_date"),
        F.col("trip_hour").cast("int").alias("trip_hour"),
        F.col("temperature_2m").cast("double").alias("temperature_2m"),
        F.col("relative_humidity_2m").cast("double").alias("relative_humidity_2m"),
        F.col("precipitation").cast("double").alias("precipitation"),
        F.col("snowfall").cast("double").alias("snowfall"),
        F.col("weather_code").cast("double").alias("weather_code"),
        F.col("wind_speed_10m").cast("double").alias("wind_speed_10m"),
        F.col("wind_gusts_10m").cast("double").alias("wind_gusts_10m")
    )
    .dropDuplicates(["trip_date", "trip_hour"])
    .withColumn("weather_available", F.lit(1))
)

if MODEL_START_DATE:
    weather_df = weather_df.filter(
        F.col("trip_date") >= F.to_date(F.lit(MODEL_START_DATE))
    )

if MODEL_END_DATE:
    weather_df = weather_df.filter(
        F.col("trip_date") <= F.to_date(F.lit(MODEL_END_DATE))
    )
# Modeling dataset preparation
observed_df = (
    agg_df
    .select(
        F.col("start_station_id").cast("string").alias("start_station_id"),
        F.col("start_station_name").cast("string").alias("start_station_name"),
        F.to_date("trip_date").alias("trip_date"),
        F.col("trip_hour").cast("int").alias("trip_hour"),
        F.col(RAW_TARGET_COL).cast("double").alias(MODEL_TARGET_COL)
    )
    .filter(F.col("start_station_id").isNotNull())
    .filter(F.trim(F.col("start_station_id")) != "")
    .filter(F.col("trip_date").isNotNull())
    .filter(F.col("trip_hour").isNotNull())
)

if MODEL_START_DATE:
    observed_df = observed_df.filter(
        F.col("trip_date") >= F.to_date(F.lit(MODEL_START_DATE))
    )

if MODEL_END_DATE:
    observed_df = observed_df.filter(
        F.col("trip_date") <= F.to_date(F.lit(MODEL_END_DATE))
    )

if STATION_LIMIT is not None:
    selected_stations_df = (
        observed_df
        .groupBy("start_station_id", "start_station_name")
        .agg(F.sum(MODEL_TARGET_COL).alias("total_departures"))
        .orderBy(F.desc("total_departures"))
        .limit(STATION_LIMIT)
        .select("start_station_id", "start_station_name")
    )

    observed_df = observed_df.join(
        selected_stations_df,
        on=["start_station_id", "start_station_name"],
        how="inner"
    )

    print(f"Limited modeling dataset to top {STATION_LIMIT} stations by departures.")


stations_df = (
    observed_df
    .select("start_station_id", "start_station_name")
    .dropDuplicates()
)

dates_df = (
    observed_df
    .select("trip_date")
    .dropDuplicates()
)

hours_df = (
    spark.range(24)
    .select(F.col("id").cast("int").alias("trip_hour"))
)

dense_grid_df = (
    stations_df
    .crossJoin(dates_df)
    .crossJoin(hours_df)
)

dense_df = (
    dense_grid_df
    .join(
        observed_df,
        on=["start_station_id", "start_station_name", "trip_date", "trip_hour"],
        how="left"
    )
    .fillna({MODEL_TARGET_COL: 0.0})
    .withColumn("day_of_week", F.dayofweek("trip_date"))
    .withColumn("year_num", F.year("trip_date"))
    .withColumn("month_num", F.month("trip_date"))
)
# Join weather after dense grid so zero-demand station-hours also get weather
dense_df = (
    dense_df
    .join(
        weather_df,
        on=["trip_date", "trip_hour"],
        how="left"
    )
)

missing_weather_rows = dense_df.filter(F.col("weather_available").isNull()).count()

if missing_weather_rows > 0:
    raise ValueError(
        f"Weather join failed for {missing_weather_rows} dense rows. "
        f"Load weather_hourly for the full modeling date range before training."
    )

dense_df = dense_df.drop("weather_available")

# Extra weather-derived features
dense_df = (
    dense_df
    .withColumn("has_precipitation", F.when(F.col("precipitation") > 0, 1).otherwise(0))
    .withColumn("has_snowfall", F.when(F.col("snowfall") > 0, 1).otherwise(0))
    .withColumn("is_freezing", F.when(F.col("temperature_2m") <= 0, 1).otherwise(0))
    .withColumn("is_windy", F.when(F.col("wind_speed_10m") >= 25, 1).otherwise(0))
)

dense_df = dense_df.cache()
# Dense grid validation
station_count = stations_df.count()
date_count = dates_df.count()
hour_count = hours_df.count()

expected_dense_rows = station_count * date_count * hour_count
actual_dense_rows = dense_df.count()

print("=== DENSE GRID VALIDATION ===")
print(f"Distinct stations: {station_count}")
print(f"Distinct dates: {date_count}")
print(f"Hours per day: {hour_count}")
print(f"Expected dense rows: {expected_dense_rows}")
print(f"Actual dense rows:   {actual_dense_rows}")
print(f"Row count match:     {expected_dense_rows == actual_dense_rows}")
print(f"Missing weather rows: {missing_weather_rows}")

dense_df.select(
    F.sum(F.when(F.col(MODEL_TARGET_COL) == 0, 1).otherwise(0)).alias("zero_demand_rows"),
    F.sum(F.when(F.col(MODEL_TARGET_COL) > 0, 1).otherwise(0)).alias("nonzero_demand_rows"),
    F.sum(F.when(F.col(MODEL_TARGET_COL).isNull(), 1).otherwise(0)).alias("null_departure_rows")
).show()

if RUN_MODE == "dev":
    dense_df.orderBy("trip_date", "trip_hour", "start_station_id").show(20, truncate=False)
else:
    print("Skipping dense_df preview in prod.")


model_df = (
    dense_df
    .withColumn("trip_date", F.to_date("trip_date"))
    .withColumn("trip_hour", F.col("trip_hour").cast("int"))
    .withColumn(MODEL_TARGET_COL, F.col(MODEL_TARGET_COL).cast("double"))
    .withColumn("dow_num", F.dayofweek("trip_date"))     # 1=Sun ... 7=Sat
    .withColumn("month_num", F.month("trip_date"))
    .withColumn("is_weekend", F.when(F.col("dow_num").isin(1, 7), 1).otherwise(0))
    .withColumn(
        "hour_ts",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.col("trip_date").cast("string"),
                F.concat(F.lpad(F.col("trip_hour").cast("string"), 2, "0"), F.lit(":00:00"))
            )
        )
    )
)

window_spec = Window.partitionBy("start_station_id").orderBy("hour_ts")

feat_df = (
    model_df
    .withColumn("lag_24", F.lag(MODEL_TARGET_COL, 24).over(window_spec))
    .withColumn("lag_168", F.lag(MODEL_TARGET_COL, 168).over(window_spec))
    .withColumn("roll_mean_24", F.avg(MODEL_TARGET_COL).over(window_spec.rowsBetween(-24, -1)))
    .withColumn("roll_mean_168", F.avg(MODEL_TARGET_COL).over(window_spec.rowsBetween(-168, -1)))
    .withColumn("roll_std_168", F.stddev_pop(MODEL_TARGET_COL).over(window_spec.rowsBetween(-168, -1)))
)

feat_df = (
    feat_df
    .filter(F.col("lag_168").isNotNull())
    .fillna({
        "lag_24": 0.0,
        "lag_168": 0.0,
        "roll_mean_24": 0.0,
        "roll_mean_168": 0.0,
        "roll_std_168": 0.0
    })
)

max_date = feat_df.select(F.max("trip_date").alias("max_date")).first()["max_date"]

test_start = max_date - timedelta(days=TEST_DAYS - 1)
valid_start = test_start - timedelta(days=VALID_DAYS)
train_start = valid_start - timedelta(days=TRAIN_LOOKBACK_DAYS)

feat_df = feat_df.filter(F.col("trip_date") >= F.lit(train_start))

train_df = feat_df.filter(
    (F.col("trip_date") >= F.lit(train_start)) & (F.col("trip_date") < F.lit(valid_start))
)

valid_df = feat_df.filter(
    (F.col("trip_date") >= F.lit(valid_start)) & (F.col("trip_date") < F.lit(test_start))
)

test_df = feat_df.filter(
    F.col("trip_date") >= F.lit(test_start)
)

train_count = train_df.count()
valid_count = valid_df.count()
test_count = test_df.count()

print("=== SPLIT VALIDATION ===")
print(f"train_start: {train_start}")
print(f"valid_start: {valid_start}")
print(f"test_start:  {test_start}")
print(f"max_date:    {max_date}")
print(f"Train rows:  {train_count}")
print(f"Valid rows:  {valid_count}")
print(f"Test rows:   {test_count}")

if train_count == 0 or valid_count == 0 or test_count == 0:
    raise ValueError(
        f"One or more split datasets are empty. "
        f"train_count={train_count}, valid_count={valid_count}, test_count={test_count}"
    )
# Baseline model
baseline_lookup = (
    train_df
    .groupBy("start_station_id", "trip_hour", "dow_num")
    .agg(F.avg(MODEL_TARGET_COL).alias("baseline_pred"))
)

fallback_lookup = (
    train_df
    .groupBy("trip_hour", "dow_num")
    .agg(F.avg(MODEL_TARGET_COL).alias("fallback_pred"))
)

baseline_test = (
    test_df
    .join(baseline_lookup, ["start_station_id", "trip_hour", "dow_num"], "left")
    .join(fallback_lookup, ["trip_hour", "dow_num"], "left")
    .withColumn(
        "prediction",
        F.coalesce(F.col("baseline_pred"), F.col("fallback_pred"), F.lit(0.0))
    )
)
# Tree-based model
station_indexer = StringIndexer(
    inputCol="start_station_id",
    outputCol="station_idx",
    handleInvalid="keep"
)

feature_cols = [
    "station_idx",
    "trip_hour",
    "dow_num",
    "month_num",
    "is_weekend",
    "lag_24",
    "lag_168",
    "roll_mean_24",
    "roll_mean_168",
    "roll_std_168",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
    "has_precipitation",
    "has_snowfall",
    "is_freezing",
    "is_windy"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol=MODEL_TARGET_COL,
    predictionCol="prediction",
    numTrees=20,
    maxDepth=7,
    maxBins=3400,
    seed=42
)

pipeline = Pipeline(stages=[station_indexer, assembler, rf])
model = pipeline.fit(train_df)

pred_valid = (
    model.transform(valid_df)
    .withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0)))
)

pred_test = (
    model.transform(test_df)
    .withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0)))
)
# Metrics
def get_metrics(pred_df, label_col=MODEL_TARGET_COL, pred_col="prediction"):
    overall = pred_df.select(
        F.avg(F.abs(F.col(label_col) - F.col(pred_col))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col(label_col) - F.col(pred_col), 2))).alias("rmse"),
        (
            (F.sum(F.abs(F.col(label_col) - F.col(pred_col))) / F.sum(F.col(label_col))) * 100.0
        ).alias("wmape_pct")
    ).first()

    nonzero_df = pred_df.filter(F.col(label_col) > 0)

    nonzero = nonzero_df.select(
        F.avg(F.abs(F.col(label_col) - F.col(pred_col))).alias("nz_mae"),
        F.sqrt(F.avg(F.pow(F.col(label_col) - F.col(pred_col), 2))).alias("nz_rmse"),
        (
            F.avg(F.abs((F.col(label_col) - F.col(pred_col)) / F.col(label_col))) * 100.0
        ).alias("nz_mape")
    ).first()

    return {
        "mae": float(overall["mae"]) if overall["mae"] is not None else None,
        "rmse": float(overall["rmse"]) if overall["rmse"] is not None else None,
        "wmape_pct": float(overall["wmape_pct"]) if overall["wmape_pct"] is not None else None,
        "nz_mae": float(nonzero["nz_mae"]) if nonzero["nz_mae"] is not None else None,
        "nz_rmse": float(nonzero["nz_rmse"]) if nonzero["nz_rmse"] is not None else None,
        "nz_mape": float(nonzero["nz_mape"]) if nonzero["nz_mape"] is not None else None,
    }


baseline_metrics = get_metrics(baseline_test, label_col=MODEL_TARGET_COL)
model_metrics = get_metrics(pred_test, label_col=MODEL_TARGET_COL)

run_ts_utc = datetime.now(timezone.utc)
run_id = run_ts_utc.strftime("%Y%m%dT%H%M%SZ") + "-" + str(uuid.uuid4())[:8]
run_date = run_ts_utc.strftime("%Y-%m-%d")

base_metadata = {
    "run_id": run_id,
    "run_ts_utc": run_ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "run_date": run_date,
    "run_mode": RUN_MODE,
    "experiment_name": EXPERIMENT_NAME,
    "feature_set_name": FEATURE_SET_NAME,
    "model_family": MODEL_FAMILY,
    "weather_enabled": WEATHER_ENABLED,
    "train_start": train_start.isoformat(),
    "valid_start": valid_start.isoformat(),
    "test_start": test_start.isoformat(),
    "max_date": max_date.isoformat(),
    "train_rows": train_count,
    "valid_rows": valid_count,
    "test_rows": test_count,
    "station_count": station_count,
    "date_count": date_count,
    "dense_rows": actual_dense_rows,
    "train_lookback_days": TRAIN_LOOKBACK_DAYS,
    "valid_days": VALID_DAYS,
    "test_days": TEST_DAYS
}

results_rows = [
    {
        **base_metadata,
        "model_name": "baseline",
        "mae": baseline_metrics["mae"],
        "rmse": baseline_metrics["rmse"],
        "wmape_pct": baseline_metrics["wmape_pct"],
        "nz_mae": baseline_metrics["nz_mae"],
        "nz_rmse": baseline_metrics["nz_rmse"],
        "nz_mape": baseline_metrics["nz_mape"]
    },
    {
        **base_metadata,
        "model_name": "improved_model_with_weather",
        "mae": model_metrics["mae"],
        "rmse": model_metrics["rmse"],
        "wmape_pct": model_metrics["wmape_pct"],
        "nz_mae": model_metrics["nz_mae"],
        "nz_rmse": model_metrics["nz_rmse"],
        "nz_mape": model_metrics["nz_mape"]
    }
]

pdf = pd.DataFrame(results_rows)

print("\n--- FINAL METRICS ---")
print(pdf.to_string(index=False))
# 9 Save outputs
metrics_sdf = spark.createDataFrame(pdf)

if WRITE_METRICS:
    metrics_dyf = DynamicFrame.fromDF(metrics_sdf, glueContext, "metrics_dyf")

    metrics_sink = glueContext.getSink(
        connection_type="s3",
        path=METRICS_TABLE_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["run_date"],
        transformation_ctx="model_metrics_sink"
    )

    metrics_sink.setCatalogInfo(
        catalogDatabase=DATABASE_NAME,
        catalogTableName=METRICS_TABLE_NAME
    )

    metrics_sink.setFormat("glueparquet", compression="snappy")
    metrics_sink.writeFrame(metrics_dyf)

    print(f"Metrics appended to {DATABASE_NAME}.{METRICS_TABLE_NAME}")
    print(f"Metrics table path: {METRICS_TABLE_PATH}")

    metrics_sdf.coalesce(1).write.mode("overwrite").option("header", "true").csv(LATEST_METRICS_OUTPUT_PATH)
    print(f"Latest metrics CSV saved to {LATEST_METRICS_OUTPUT_PATH}")
else:
    print("WRITE_METRICS=false, skipping metrics table and latest CSV writes.")
job.commit()
