import sys
import calendar
from datetime import date
import requests
import pandas as pd

from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "target_start_yyyymm", "target_end_yyyymm"]
)

target_start_yyyymm = args["target_start_yyyymm"]
target_end_yyyymm = args["target_end_yyyymm"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DATABASE = "divvy_db"
TABLE = "weather_hourly"
OUTPUT_PATH = "s3://gbender-divvy-data-platform/curated/weather-hourly/"
LATITUDE = 41.8781
LONGITUDE = -87.6298
TIMEZONE = "America/Chicago"


def parse_yyyymm(yyyymm: str):
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"Invalid YYYYMM value: {yyyymm}")
    year_num = int(yyyymm[:4])
    month_num = int(yyyymm[4:6])
    if month_num < 1 or month_num > 12:
        raise ValueError(f"Invalid month in YYYYMM: {yyyymm}")
    return year_num, month_num


def month_range(start_yyyymm: str, end_yyyymm: str):
    start_year, start_month = parse_yyyymm(start_yyyymm)
    end_year, end_month = parse_yyyymm(end_yyyymm)

    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(
            f"target_start_yyyymm must be <= target_end_yyyymm. "
            f"Got {start_yyyymm} > {target_end_yyyymm}"
        )

    months = []
    year_num, month_num = start_year, start_month

    while (year_num, month_num) <= (end_year, end_month):
        months.append((year_num, month_num))
        if month_num == 12:
            year_num += 1
            month_num = 1
        else:
            month_num += 1

    return months


def get_date_range(start_yyyymm: str, end_yyyymm: str):
    start_year, start_month = parse_yyyymm(start_yyyymm)
    end_year, end_month = parse_yyyymm(end_yyyymm)

    first_day = date(start_year, start_month, 1)
    last_day_num = calendar.monthrange(end_year, end_month)[1]
    last_day = date(end_year, end_month, last_day_num)

    return first_day.isoformat(), last_day.isoformat()


months_to_refresh = month_range(target_start_yyyymm, target_end_yyyymm)
start_date, end_date = get_date_range(target_start_yyyymm, target_end_yyyymm)

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "start_date": start_date,
    "end_date": end_date,
    "hourly": ",".join([
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "snowfall",
        "weather_code",
        "wind_speed_10m",
        "wind_gusts_10m"
    ]),
    "timezone": TIMEZONE
}

response = requests.get(url, params=params, timeout=60)
response.raise_for_status()
data = response.json()
hourly = data["hourly"]

weather_df = pd.DataFrame({
    "weather_ts": pd.to_datetime(hourly["time"]),
    "temperature_2m": hourly["temperature_2m"],
    "relative_humidity_2m": hourly["relative_humidity_2m"],
    "precipitation": hourly["precipitation"],
    "snowfall": hourly["snowfall"],
    "weather_code": hourly["weather_code"],
    "wind_speed_10m": hourly["wind_speed_10m"],
    "wind_gusts_10m": hourly["wind_gusts_10m"],
})

if weather_df.empty:
    job.commit()
    raise RuntimeError(
        f"No weather rows returned for {target_start_yyyymm} through {target_end_yyyymm}"
    )

weather_df["trip_date"] = pd.to_datetime(weather_df["weather_ts"]).dt.date
weather_df["trip_hour"] = pd.to_datetime(weather_df["weather_ts"]).dt.hour
weather_df["year_num"] = pd.to_datetime(weather_df["weather_ts"]).dt.year
weather_df["month_num"] = pd.to_datetime(weather_df["weather_ts"]).dt.month
weather_df["source_latitude"] = LATITUDE
weather_df["source_longitude"] = LONGITUDE
weather_df["source_timezone"] = TIMEZONE

weather_spark_df = spark.createDataFrame(weather_df)

weather_spark_df = (
    weather_spark_df
    .withColumn("weather_ts", col("weather_ts").cast("timestamp"))
    .withColumn("trip_date", to_date(col("trip_date")))
    .withColumn("trip_hour", col("trip_hour").cast("int"))
    .withColumn("relative_humidity_2m", col("relative_humidity_2m").cast("int"))
    .withColumn("weather_code", col("weather_code").cast("int"))
    .withColumn("year_num", col("year_num").cast("int"))
    .withColumn("month_num", col("month_num").cast("int"))
    .withColumn("source_latitude", col("source_latitude").cast("double"))
    .withColumn("source_longitude", col("source_longitude").cast("double"))
)

# purge only the month partitions being refreshed
for year_num, month_num in months_to_refresh:
    partition_path = f"{OUTPUT_PATH}year_num={year_num}/month_num={month_num}/"
    glueContext.purge_s3_path(
        partition_path,
        options={"retentionPeriod": 0}
    )

weather_dyf = DynamicFrame.fromDF(weather_spark_df, glueContext, "weather_dyf")

sink = glueContext.getSink(
    connection_type="s3",
    path=OUTPUT_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year_num", "month_num"],
    compression="snappy"
)

sink.setCatalogInfo(
    catalogDatabase=DATABASE,
    catalogTableName=TABLE
)

sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(weather_dyf)

job.commit()
