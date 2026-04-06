import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    dayofweek,
    year,
    month,
    count
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def get_optional_arg(name, default=None):
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


args = getResolvedOptions(sys.argv, ["JOB_NAME", "rebuild_mode"])
rebuild_mode = args["rebuild_mode"].upper()

run_year = get_optional_arg("run_year")
run_month = get_optional_arg("run_month")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

base_path = "s3://gbender-wgu-capstone/processed/station_hour_departures/"

# Read cleaned trip-level data
divvy_clean = glueContext.create_dynamic_frame.from_catalog(
    database="divvy_db",
    table_name="divvy_clean",
    transformation_ctx="divvy_clean"
)

clean_df = divvy_clean.toDF()

if clean_df.rdd.isEmpty():
    print("No data found in divvy_clean. Nothing to aggregate.")
else:
    prepared_df = (
        clean_df
        .filter(col("started_at").isNotNull())
        .filter(col("start_station_id").isNotNull())
        .filter(col("start_station_name").isNotNull())
        .filter(col("start_station_id") != "")
        .filter(col("start_station_name") != "")
        .filter(col("ride_id").isNotNull())
        .withColumn("trip_date", to_date(col("started_at")))
        .withColumn("trip_hour", hour(col("started_at")))
        .withColumn("day_of_week", dayofweek(col("started_at")))
        .withColumn("year_num", year(col("started_at")))
        .withColumn("month_num", month(col("started_at")))
    )

    prepared_df = prepared_df.dropDuplicates(["ride_id"])

    paths_to_purge = []
    target_df = None

    if rebuild_mode == "FULL":
        print("Running FULL rebuild of station_hour_departures")
        target_df = prepared_df
        paths_to_purge = [base_path]

    elif rebuild_mode == "LAST_2_MONTHS":
        if run_year is not None and run_month is not None:
            current_year = int(run_year)
            current_month = int(run_month)
        else:
            now = datetime.now()
            current_year = now.year
            current_month = now.month

        if current_month == 1:
            month1_year, month1 = current_year - 1, 12
            month2_year, month2 = current_year - 1, 11
        elif current_month == 2:
            month1_year, month1 = current_year, 1
            month2_year, month2 = current_year - 1, 12
        else:
            month1_year, month1 = current_year, current_month - 1
            month2_year, month2 = current_year, current_month - 2

        print(f"Rebuilding aggregates for {month2_year}-{month2:02d} and {month1_year}-{month1:02d}")

        target_df = prepared_df.filter(
            ((col("year_num") == month1_year) & (col("month_num") == month1)) |
            ((col("year_num") == month2_year) & (col("month_num") == month2))
        )

        paths_to_purge = [
            f"{base_path}year_num={month1_year}/month_num={month1}/",
            f"{base_path}year_num={month2_year}/month_num={month2}/"
        ]

    else:
        raise ValueError("Invalid rebuild_mode. Use FULL or LAST_2_MONTHS.")

    if target_df is None or target_df.rdd.isEmpty():
        print("No rows found for the selected rebuild window. Nothing to aggregate.")
    else:
        for path in paths_to_purge:
            print(f"Purging path: {path}")
            glueContext.purge_s3_path(path, options={"retentionPeriod": 0})

        agg_df = (
            target_df
            .groupBy(
                "start_station_id",
                "start_station_name",
                "trip_date",
                "trip_hour",
                "day_of_week",
                "year_num",
                "month_num"
            )
            .agg(count("*").alias("station_hour_departures"))
        )

        agg_dyf = DynamicFrame.fromDF(agg_df, glueContext, "agg_dyf")

        sink = glueContext.getSink(
            path=base_path,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["year_num", "month_num"],
            enableUpdateCatalog=True,
            transformation_ctx="station_hour_departures_sink"
        )

        sink.setCatalogInfo(
            catalogDatabase="divvy_db",
            catalogTableName="station_hour_departures"
        )
        sink.setFormat("glueparquet", compression="snappy")
        sink.writeFrame(agg_dyf)

        print("Aggregation write completed successfully.")

job.commit()
