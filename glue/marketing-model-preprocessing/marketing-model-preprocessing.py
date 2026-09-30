
import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import (
    col, when, lit, current_date,
    datediff, to_date
)

# ============================================================
# 1. Initialize Glue / Spark
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. S3 paths
# ============================================================

input_path = "s3://bank-marketing-ml-poc-vineet/raw/"

output_path = (
    "s3://bank-marketing-ml-poc-vineet/"
    "processed/marketing_features/"
)


# ============================================================
# 3. Read data from S3
# ============================================================

print("Reading raw data from S3")

df = spark.read.parquet(input_path)

print(f"Input records: {df.count():,}")

df.printSchema()


# ============================================================
# 4. Data type conversion
# ============================================================

numeric_columns = [
    "AGE",
    "ANNUAL_INCOME",
    "TENURE_MONTHS",
    "CREDIT_SCORE",
    "EXISTING_PRODUCTS",
    "MONTHLY_SPEND",
    "AVG_MONTHLY_BALANCE",
    "DIGITAL_TXN_RATIO",
    "MOBILE_LOGINS_30D",
    "BRANCH_VISITS_6M",
    "WEB_VISITS_30D",
    "CREDIT_UTILIZATION",
    "DELINQUENCY_12M",
    "MARKETING_CONTACTS_90D",
    "DAYS_SINCE_LAST_TXN"
]

for column in numeric_columns:
    df = df.withColumn(
        column,
        col(column).cast("double")
    )


# ============================================================
# 5. Handle missing values
# ============================================================

df = df.fillna({
    "AGE": 0,
    "ANNUAL_INCOME": 0,
    "TENURE_MONTHS": 0,
    "CREDIT_SCORE": 0,
    "EXISTING_PRODUCTS": 0,
    "MONTHLY_SPEND": 0,
    "AVG_MONTHLY_BALANCE": 0,
    "DIGITAL_TXN_RATIO": 0,
    "MOBILE_LOGINS_30D": 0,
    "BRANCH_VISITS_6M": 0,
    "WEB_VISITS_30D": 0,
    "CREDIT_UTILIZATION": 0,
    "DELINQUENCY_12M": 0,
    "MARKETING_CONTACTS_90D": 0,
    "DAYS_SINCE_LAST_TXN": 0
})

df = df.fillna({
    "CUSTOMER_SEGMENT": "UNKNOWN",
    "ACQUISITION_CHANNEL": "UNKNOWN",
    "EMPLOYMENT_TYPE": "UNKNOWN"
})


# ============================================================
# 6. Feature engineering
# ============================================================

print("Creating marketing features")


# Feature 1: Monthly income
df = df.withColumn(
    "MONTHLY_INCOME",
    col("ANNUAL_INCOME") / lit(12.0)
)


# Feature 2: Spend to income ratio
df = df.withColumn(
    "SPEND_INCOME_RATIO",
    when(
        col("MONTHLY_INCOME") > 0,
        col("MONTHLY_SPEND") / col("MONTHLY_INCOME")
    ).otherwise(lit(0.0))
)


# Feature 3: Digital engagement score
df = df.withColumn(
    "DIGITAL_ENGAGEMENT_SCORE",
    col("MOBILE_LOGINS_30D") +
    col("WEB_VISITS_30D")
)


# Feature 4: Average spend per product
df = df.withColumn(
    "SPEND_PER_PRODUCT",
    when(
        col("EXISTING_PRODUCTS") > 0,
        col("MONTHLY_SPEND") /
        col("EXISTING_PRODUCTS")
    ).otherwise(lit(0.0))
)


# Feature 5: Customer activity flag
df = df.withColumn(
    "ACTIVE_CUSTOMER_FLAG",
    when(
        col("DAYS_SINCE_LAST_TXN") <= 30,
        lit(1)
    ).otherwise(lit(0))
)


# ============================================================
# 7. Convert campaign date
# ============================================================

df = df.withColumn(
    "CAMPAIGN_DATE",
    to_date(col("CAMPAIGN_DATE"))
)


# ============================================================
# 8. Validation
# ============================================================

print("Validating processed data")

processed_count = df.count()

print(f"Processed records: {processed_count:,}")

df.printSchema()

df.show(10, truncate=False)


# ============================================================
# 9. Write processed data to S3
# ============================================================

print("Writing processed data to S3")

df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Processed data written successfully")


# ============================================================
# 10. Commit Glue job
# ============================================================

job.commit()

print("Glue preprocessing completed successfully")