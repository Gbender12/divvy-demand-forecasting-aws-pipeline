import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import regexp_replace, to_timestamp, year, month
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw from Glue Data Catalog
divvy_raw = glueContext.create_dynamic_frame.from_catalog(
    database="divvy_db",
    table_name="raw",
    transformation_ctx="divvy_raw"
)

raw_df = divvy_raw.toDF()

if raw_df.rdd.isEmpty():
    print("No new data to process. Nothing will be written.")
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
        .withColumn("year_num", year("started_at"))
        .withColumn("month_num", month("started_at"))
    )

    clean_dyf = DynamicFrame.fromDF(
        clean_df,
        glueContext,
        "clean_dyf"
    )

    sink = glueContext.getSink(
        path="s3://gbender-wgu-capstone/processed/divvy_clean/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["year_num", "month_num"],
        enableUpdateCatalog=True,
        transformation_ctx="divvy_clean_sink"
    )

    sink.setCatalogInfo(
        catalogDatabase="divvy_db",
        catalogTableName="divvy_clean"
    )

    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(clean_dyf)

job.commit()
