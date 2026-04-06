import os
import sys
import boto3
import pandas as pd
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
except Exception:
    args = {'JOB_NAME': 'local_notebook_run'}
    
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Configuration
RUN_MODE = os.getenv("RUN_MODE", "glue")   # local or glue

DATABASE_NAME = "divvy_db"
TABLE_NAME = "station_hour_departures"

RAW_TARGET_COL = "station_hour_departures"
MODEL_TARGET_COL = "departures"

AWS_PROFILE = "default"
AWS_REGION = "us-east-1"

LOCAL_CACHE_PATH = "./data/station_hour_departures_subset.parquet"

METRICS_OUTPUT_PATH = "s3://gbender-wgu-capstone/analysis/model_metrics/"
PREDICTIONS_OUTPUT_PATH = "s3://gbender-wgu-capstone/analysis/model_predictions/"
# Session creation
def get_spark_session(run_mode: str):
    if run_mode == "glue":
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        return spark, glue_context

    spark = (
        SparkSession.builder
        .appName("divvy-local-model")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.default.parallelism", "32")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    return spark, None
# Helpers for local AWS access
def get_boto3_session():
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

def get_glue_table_s3_location(session, database_name: str, table_name: str) -> str:
    glue = session.client("glue")
    resp = glue.get_table(DatabaseName=database_name, Name=table_name)
    return resp["Table"]["StorageDescriptor"]["Location"]
# Data loading
def load_aggregate_table(run_mode: str, spark, glue_context, database_name: str, table_name: str):
    if run_mode == "glue":
        return spark.table(f"{database_name}.{table_name}")

    if not os.path.exists(LOCAL_CACHE_PATH):
        raise FileNotFoundError(
            f"Local cache not found at {LOCAL_CACHE_PATH}. "
            f"Create a local Parquet subset first."
        )

    print("Reading local mode from:", LOCAL_CACHE_PATH)
    return spark.read.parquet(LOCAL_CACHE_PATH)
# Main
spark, glue_context = get_spark_session(RUN_MODE)

agg_df = load_aggregate_table(
    run_mode=RUN_MODE,
    spark=spark,
    glue_context=glue_context,
    database_name=DATABASE_NAME,
    table_name=TABLE_NAME
)

if RAW_TARGET_COL not in agg_df.columns:
    raise ValueError(
        f"Expected raw target column '{RAW_TARGET_COL}' not found. "
        f"Available columns: {agg_df.columns}"
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

dense_df = dense_df.cache()


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

dense_df.select(
    F.sum(F.when(F.col(MODEL_TARGET_COL) == 0, 1).otherwise(0)).alias("zero_demand_rows"),
    F.sum(F.when(F.col(MODEL_TARGET_COL) > 0, 1).otherwise(0)).alias("nonzero_demand_rows"),
    F.sum(F.when(F.col(MODEL_TARGET_COL).isNull(), 1).otherwise(0)).alias("null_departure_rows")
).show()

dense_df.orderBy("trip_date", "trip_hour", "start_station_id").show(20, truncate=False)




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

test_start = max_date - timedelta(days=27)      # last 28 days
valid_start = max_date - timedelta(days=55)     # previous 28 days
train_start = max_date - timedelta(days=235)    # ~180 days before validation

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
# 7 Hashed feature model Baseline 
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
# 8 Tree-Based Evaluation helpers
station_indexer = StringIndexer(inputCol="start_station_id", outputCol="station_idx", handleInvalid="keep")

feature_cols = [
    "station_idx", "trip_hour", "dow_num", "month_num", 
    "is_weekend", "lag_24", "lag_168", "roll_mean_24", 
    "roll_mean_168", "roll_std_168"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# REDUCED MEMORY FOOTPRINT: numTrees = 20 and maxDepth = 7
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
# 9 Save outputs
def get_metrics(pred_df, label_col=MODEL_TARGET_COL, pred_col="prediction"):
    overall = pred_df.select(
        F.avg(F.abs(F.col(label_col) - F.col(pred_col))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col(label_col) - F.col(pred_col), 2))).alias("rmse"),
        ((F.sum(F.abs(F.col(label_col) - F.col(pred_col))) / F.sum(F.col(label_col))) * 100.0).alias("wmape_pct")
    ).first()

    nonzero_df = pred_df.filter(F.col(label_col) > 0)

    nonzero = nonzero_df.select(
        F.avg(F.abs(F.col(label_col) - F.col(pred_col))).alias("nz_mae"),
        F.sqrt(F.avg(F.pow(F.col(label_col) - F.col(pred_col), 2))).alias("nz_rmse"),
        (F.avg(F.abs((F.col(label_col) - F.col(pred_col)) / F.col(label_col))) * 100.0).alias("nz_mape")
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

# Bypass PySpark worker memory overhead by using native Pandas for the small table
results_rows = [
    {
        "model_name": "baseline",
        "mae": baseline_metrics["mae"],
        "rmse": baseline_metrics["rmse"],
        "wmape_pct": baseline_metrics["wmape_pct"],
        "nz_mae": baseline_metrics["nz_mae"],
        "nz_rmse": baseline_metrics["nz_rmse"],
        "nz_mape": baseline_metrics["nz_mape"]
    },
    {
        "model_name": "improved_model",
        "mae": model_metrics["mae"],
        "rmse": model_metrics["rmse"],
        "wmape_pct": model_metrics["wmape_pct"],
        "nz_mae": model_metrics["nz_mae"],
        "nz_rmse": model_metrics["nz_rmse"],
        "nz_mape": model_metrics["nz_mape"]
    }
]

# Print out using Pandas
pdf = pd.DataFrame(results_rows)
print("\n--- FINAL METRICS ---")
print(pdf.to_string(index=False))

# Setup paths
if RUN_MODE == "glue":
    pred_test.select(
        "trip_date", "start_station_id", "trip_hour", MODEL_TARGET_COL, "prediction"
    ).coalesce(4).write.mode("overwrite").parquet(PREDICTIONS_OUTPUT_PATH)
    print(f"Predictions saved to {PREDICTIONS_OUTPUT_PATH} via PySpark")
    
    metrics_sdf = spark.createDataFrame(pdf)
    metrics_sdf.coalesce(1).write.mode("overwrite").option("header", "true").csv(METRICS_OUTPUT_PATH)

else:
    # For local Windows. Bypass Spark's Hadoop dependency by using Pandas
    print("Converting predictions to Pandas to bypass Windows Hadoop requirement...")
    
    pred_pdf = pred_test.select(
        "trip_date", "start_station_id", "trip_hour", MODEL_TARGET_COL, "prediction"
    ).toPandas()
    
    # Save locally using Pandas
    local_file_path = os.path.join(PREDICTIONS_OUTPUT_PATH, "predictions.parquet")
    pred_pdf.to_parquet(local_file_path, index=False)
    
    print(f"Predictions saved to {local_file_path} via Pandas")
job.commit()
