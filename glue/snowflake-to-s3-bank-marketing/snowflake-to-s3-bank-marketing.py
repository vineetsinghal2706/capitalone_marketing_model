import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# 1. Initialize Glue / Spark
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args["JOB_NAME"], args)


# ============================================================
# 2. Snowflake configuration
# ============================================================
#
# Credentials are NOT stored in this script.
#
# AWS Glue will use:
#   Glue Connection -> Snowflake connection
#   Secrets Manager -> USERNAME / PASSWORD
#
# ============================================================

snowflake_options = {

    # Name of the AWS Glue Snowflake connection
    "connectionName": "Snowflake connection",

    # Snowflake database
    "sfDatabase": "INGEST_DATA",

    # Snowflake schema
    "sfSchema": "PUBLIC",

    # Snowflake warehouse
    "sfWarehouse": "COMPUTE_WH",

    # Enable Snowflake query pushdown
    "autopushdown": "on",

    # SQL query
    "query": """
        SELECT
            *
        FROM INGEST_DATA.PUBLIC.CUSTOMERS
        WHERE CUSTOMER_ID IS NOT NULL
    """
}


# ============================================================
# 3. Read data from Snowflake
# ============================================================

print("================================================")
print("Starting Snowflake read...")
print("================================================")

snowflake_dynamic_frame = (
    glueContext.create_dynamic_frame.from_options(
        connection_type="snowflake",
        connection_options=snowflake_options,
        transformation_ctx="snowflake_source"
    )
)

print("Snowflake read completed successfully.")


# ============================================================
# 4. Convert DynamicFrame to DataFrame
# ============================================================

df = snowflake_dynamic_frame.toDF()


# ============================================================
# 5. Display schema
# ============================================================

print("================================================")
print("Snowflake schema:")
print("================================================")

df.printSchema()


# ============================================================
# 6. Display sample records
# ============================================================

print("================================================")
print("First 10 records:")
print("================================================")

df.show(10, truncate=False)


# ============================================================
# 7. Count records
# ============================================================

print("================================================")
print("Counting records...")
print("================================================")

record_count = df.count()

print(f"Total records extracted: {record_count:,}")


# ============================================================
# 8. Write data to S3
# ============================================================

output_path = "s3://bank-marketing-ml-poc-vineet/raw/"

print("================================================")
print("Writing data to S3:")
print(output_path)
print("================================================")


# ------------------------------------------------------------
# Option 1: Recommended for large datasets
# ------------------------------------------------------------
#
# Keep multiple partitions.
# Spark will create multiple Parquet files.
#
# ------------------------------------------------------------
df = df.repartition(5)
df.write \
    .mode("overwrite") \
    .parquet(output_path)


print("Data successfully written to S3.")


# ============================================================
# 9. Commit Glue job
# ============================================================

job.commit()

print("================================================")
print("Glue job completed successfully.")
print("================================================")