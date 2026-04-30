import sys
from datetime import date, timedelta
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


def previous_month_yyyymm(today=None):
    if today is None:
        today = date.today()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    return last_day_prev_month.strftime("%Y%m")


def parse_yyyymm(yyyymm: str):
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"Invalid YYYYMM value: {yyyymm}")

    year_num = int(yyyymm[:4])
    month_num = int(yyyymm[4:6])

    if month_num < 1 or month_num > 12:
        raise ValueError(f"Invalid month in YYYYMM: {yyyymm}")

    return year_num, month_num


def format_yyyymm(year_num: int, month_num: int) -> str:
    return f"{year_num:04d}{month_num:02d}"


def shift_month(yyyymm: str, offset: int) -> str:
    year_num, month_num = parse_yyyymm(yyyymm)
    absolute_month = year_num * 12 + (month_num - 1)
    shifted = absolute_month + offset
    shifted_year = shifted // 12
    shifted_month = (shifted % 12) + 1
    return format_yyyymm(shifted_year, shifted_month)


def iter_months(start_yyyymm: str, end_yyyymm: str):
    start_year, start_month = parse_yyyymm(start_yyyymm)
    end_year, end_month = parse_yyyymm(end_yyyymm)

    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(
            f"target_start_yyyymm must be <= target_end_yyyymm. "
            f"Got {start_yyyymm} > {end_yyyymm}"
        )

    months = []
    cur_year, cur_month = start_year, start_month

    while (cur_year, cur_month) <= (end_year, end_month):
        months.append((cur_year, cur_month))
        if cur_month == 12:
            cur_year += 1
            cur_month = 1
        else:
            cur_month += 1

    return months


# only JOB_NAME is always required
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# optional args
optional_arg_names = []
if "--target_start_yyyymm" in sys.argv:
    optional_arg_names.append("target_start_yyyymm")
if "--target_end_yyyymm" in sys.argv:
    optional_arg_names.append("target_end_yyyymm")

optional_args = (
    getResolvedOptions(sys.argv, optional_arg_names)
    if optional_arg_names else {}
)

default_month = previous_month_yyyymm()
requested_start_yyyymm = optional_args.get("target_start_yyyymm", default_month)
requested_end_yyyymm = optional_args.get("target_end_yyyymm", requested_start_yyyymm)

# Always include one overlap month before the requested start month
effective_start_yyyymm = shift_month(requested_start_yyyymm, -1)
effective_end_yyyymm = requested_end_yyyymm

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_DATABASE = "divvy_db"
SOURCE_TABLE = "curated_tripdata"

TARGET_DATABASE = "divvy_db"
TARGET_TABLE = "station_hour_departures"
TARGET_PATH = "s3://gbender-divvy-data-platform/analytics/station-hour-departures/"

months_to_process = iter_months(effective_start_yyyymm, effective_end_yyyymm)

# assumes curated_tripdata is partitioned by year_num and month_num
push_down_predicate = " OR ".join(
    [
        f"(year_num == {year_num} AND month_num == {month_num})"
        for year_num, month_num in months_to_process
    ]
)

print(f"Requested range: {requested_start_yyyymm} to {requested_end_yyyymm}")
print(f"Effective aggregate range: {effective_start_yyyymm} to {effective_end_yyyymm}")
print(f"Using push_down_predicate: {push_down_predicate}")

curated_tripdata = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    transformation_ctx="curated_tripdata",
    push_down_predicate=push_down_predicate
)

trip_df = curated_tripdata.toDF()

if trip_df.rdd.isEmpty():
    print("No curated tripdata found for requested month range. Nothing will be written.")
else:
    prepared_df = (
        trip_df
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
        .dropDuplicates(["ride_id"])
    )

    target_month_predicate = None
    for year_num, month_num in months_to_process:
        condition = (
            (col("year_num") == year_num) &
            (col("month_num") == month_num)
        )
        target_month_predicate = (
            condition if target_month_predicate is None
            else (target_month_predicate | condition)
        )

    prepared_df = prepared_df.filter(target_month_predicate)

    if prepared_df.rdd.isEmpty():
        print("No rows remained after target month filter. Nothing will be written.")
    else:
        for year_num, month_num in months_to_process:
            partition_path = (
                f"{TARGET_PATH}year_num={year_num}/month_num={month_num}/"
            )
            print(f"Purging target partition path: {partition_path}")
            glueContext.purge_s3_path(
                partition_path,
                options={"retentionPeriod": 0}
            )

        agg_df = (
            prepared_df
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
            connection_type="s3",
            path=TARGET_PATH,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["year_num", "month_num"],
            enableUpdateCatalog=True,
            transformation_ctx="station_hour_departures_sink"
        )

        sink.setCatalogInfo(
            catalogDatabase=TARGET_DATABASE,
            catalogTableName=TARGET_TABLE
        )

        sink.setFormat("parquet", useGlueParquetWriter=True)
        sink.writeFrame(agg_dyf)

        print("Aggregation write completed successfully.")

job.commit()
