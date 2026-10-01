import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date"])
run_date = args["run_date"]

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


snowflake_options = {
    "connectionName": "Snowflake connection",
    "sfDatabase": "INGEST_DATA",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "autopushdown": "on",
    "query": """
        SELECT *
        FROM INGEST_DATA.PUBLIC.CUSTOMERS
        WHERE CUSTOMER_ID IS NOT NULL
    """
}

print("Starting Snowflake read...")

snowflake_dynamic_frame = (
    glueContext.create_dynamic_frame.from_options(
        connection_type="snowflake",
        connection_options=snowflake_options,
        transformation_ctx="snowflake_source"
    )
)

df = snowflake_dynamic_frame.toDF()

print("Snowflake schema:")
df.printSchema()

print("First 10 records:")
df.show(10, truncate=False)

record_count = df.count()
print(f"Total records extracted: {record_count:,}")

output_path = f"s3://bank-marketing-ml-poc-vineet/raw/{run_date}/"

print(f"Writing data to S3: {output_path}")

df = df.repartition(5)

df.write     .mode("overwrite")     .parquet(output_path)

print("Data successfully written to S3.")

job.commit()

print("Glue job completed successfully.")
