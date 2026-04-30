import sys
from datetime import date, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import regexp_replace, to_timestamp, year, month, col
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
target_start_yyyymm = optional_args.get("target_start_yyyymm", default_month)
target_end_yyyymm = optional_args.get("target_end_yyyymm", target_start_yyyymm)


target_months_to_write = iter_months(target_start_yyyymm, target_end_yyyymm)
raw_months_to_read = target_months_to_write

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_DATABASE = "divvy_db"
SOURCE_TABLE = "raw_tripdata"

TARGET_DATABASE = "divvy_db"
TARGET_TABLE = "curated_tripdata"
TARGET_PATH = "s3://gbender-divvy-data-platform/curated/tripdata/"

RAW_PARTITION_YEAR_COL = "year"
RAW_PARTITION_MONTH_COL = "month"

push_down_predicate = " OR ".join(
    [
        f"({RAW_PARTITION_YEAR_COL} == '{year_num:04d}' AND "
        f"{RAW_PARTITION_MONTH_COL} == '{month_num:02d}')"
        for year_num, month_num in raw_months_to_read
    ]
)

print(f"Target write range: {target_start_yyyymm} to {target_end_yyyymm}")
print(f"Raw read range: {target_start_yyyymm} to {target_end_yyyymm}")
print(f"Using push_down_predicate: {push_down_predicate}")

divvy_raw = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    transformation_ctx="divvy_raw",
    push_down_predicate=push_down_predicate
)

raw_df = divvy_raw.toDF()

if raw_df.rdd.isEmpty():
    print("No raw data found for requested read range. Nothing will be written.")
else:
    clean_df = (
        raw_df
        .withColumn(
            "started_at",
            to_timestamp(
                regexp_replace("started_at", '^"|"$', ''),
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
        )
        .withColumn(
            "ended_at",
            to_timestamp(
                regexp_replace("ended_at", '^"|"$', ''),
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
        )
        .filter(col("started_at").isNotNull())
        .withColumn("year_num", year("started_at"))
        .withColumn("month_num", month("started_at"))
    )

    # only write the exact requested target months
    target_month_predicate = None
    for year_num, month_num in target_months_to_write:
        condition = (
            (col("year_num") == year_num) &
            (col("month_num") == month_num)
        )
        target_month_predicate = (
            condition if target_month_predicate is None
            else (target_month_predicate | condition)
        )

    clean_df = clean_df.filter(target_month_predicate)

    if clean_df.rdd.isEmpty():
        print("No cleaned rows remained after target month filter. Nothing will be written.")
    else:
        for year_num, month_num in target_months_to_write:
            partition_path = (
                f"{TARGET_PATH}year_num={year_num}/month_num={month_num}/"
            )
            print(f"Purging target partition path: {partition_path}")
            glueContext.purge_s3_path(
                partition_path,
                options={"retentionPeriod": 0}
            )

        clean_dyf = DynamicFrame.fromDF(clean_df, glueContext, "clean_dyf")

        sink = glueContext.getSink(
            connection_type="s3",
            path=TARGET_PATH,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["year_num", "month_num"],
            transformation_ctx="curated_tripdata_sink"
        )

        sink.setCatalogInfo(
            catalogDatabase=TARGET_DATABASE,
            catalogTableName=TARGET_TABLE
        )

        sink.setFormat("parquet", useGlueParquetWriter=True)
        sink.writeFrame(clean_dyf)

job.commit()
