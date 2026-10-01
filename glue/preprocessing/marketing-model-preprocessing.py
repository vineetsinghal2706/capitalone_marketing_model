import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    log1p,
    month,
    quarter,
    dayofweek,
    sin,
    cos,
    lit,
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# 1. Initialize Glue / Spark
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "run_date"]
)

run_date = args["run_date"]

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args["JOB_NAME"], args)


# ============================================================
# 2. S3 Configuration
# ============================================================

INPUT_PATH = (
    f"s3://bank-marketing-ml-poc-vineet/"
    f"raw/{run_date}/"
)

OUTPUT_PATH = (
    f"s3://bank-marketing-ml-poc-vineet/"
    f"processed/marketing_features/{run_date}/"
)

TARGET = "campaign_response"


# ============================================================
# 3. Feature definitions
# ============================================================

NUMERIC_FEATURES = [
    "age",
    "annual_income",
    "tenure_months",
    "credit_score",
    "existing_products",
    "monthly_spend",
    "avg_monthly_balance",
    "digital_txn_ratio",
    "mobile_logins_30d",
    "branch_visits_6m",
    "web_visits_30d",
    "credit_utilization",
    "delinquency_12m",
    "marketing_contacts_90d",
    "days_since_last_txn",
]

CATEGORICAL_FEATURES = [
    "customer_segment",
    "acquisition_channel",
    "employment_type",
]


# ============================================================
# 4. Feature Engineering Function
# ============================================================

def build_features(df):

    print("Starting feature engineering...")

    # --------------------------------------------------------
    # Remove duplicate customers
    # --------------------------------------------------------

    print("Removing duplicate customers...")

    df = df.dropDuplicates(["customer_id"])


    # --------------------------------------------------------
    # Convert campaign date
    # --------------------------------------------------------

    print("Converting campaign_date to date...")

    df = df.withColumn(
        "campaign_date",
        col("campaign_date").cast("date")
    )


    # --------------------------------------------------------
    # Clean categorical columns
    # --------------------------------------------------------

    print("Cleaning categorical columns...")

    for c in CATEGORICAL_FEATURES:

        df = df.withColumn(
            c,
            lower(trim(col(c)))
        )


    # --------------------------------------------------------
    # Date features
    # --------------------------------------------------------

    print("Creating date features...")

    df = (
        df
        .withColumn(
            "campaign_month",
            month("campaign_date")
        )
        .withColumn(
            "campaign_quarter",
            quarter("campaign_date")
        )
        .withColumn(
            "campaign_dayofweek",
            dayofweek("campaign_date")
        )
        .withColumn(
            "campaign_is_weekend",
            when(
                col("campaign_dayofweek").isin(1, 7),
                1
            ).otherwise(0)
        )
        .withColumn(
            "campaign_month_sin",
            sin(
                2
                * lit(3.141592653589793)
                * col("campaign_month")
                / lit(12)
            )
        )
        .withColumn(
            "campaign_month_cos",
            cos(
                2
                * lit(3.141592653589793)
                * col("campaign_month")
                / lit(12)
            )
        )
    )


    # --------------------------------------------------------
    # Outlier capping
    # --------------------------------------------------------

    print("Applying outlier capping...")

    df = (
        df
        .withColumn(
            "annual_income",
            when(
                col("annual_income") > 250000,
                250000
            ).otherwise(
                col("annual_income")
            )
        )
        .withColumn(
            "annual_income",
            when(
                col("annual_income") < 18000,
                18000
            ).otherwise(
                col("annual_income")
            )
        )
        .withColumn(
            "monthly_spend",
            when(
                col("monthly_spend") > 20000,
                20000
            ).otherwise(
                col("monthly_spend")
            )
        )
        .withColumn(
            "monthly_spend",
            when(
                col("monthly_spend") < 100,
                100
            ).otherwise(
                col("monthly_spend")
            )
        )
        .withColumn(
            "avg_monthly_balance",
            when(
                col("avg_monthly_balance") > 100000,
                100000
            ).otherwise(
                col("avg_monthly_balance")
            )
        )
    )


    # --------------------------------------------------------
    # Log features
    # --------------------------------------------------------

    print("Creating log features...")

    df = (
        df
        .withColumn(
            "log_income",
            log1p(col("annual_income"))
        )
        .withColumn(
            "log_monthly_spend",
            log1p(col("monthly_spend"))
        )
        .withColumn(
            "log_avg_balance",
            log1p(col("avg_monthly_balance"))
        )
        .withColumn(
            "log_web_visits",
            log1p(col("web_visits_30d"))
        )
    )


    # --------------------------------------------------------
    # Ratio and engagement features
    # --------------------------------------------------------

    print("Creating ratio and engagement features...")

    df = (
        df
        .withColumn(
            "spend_to_income",
            col("monthly_spend")
            / (
                col("annual_income") / 12 + 1
            )
        )
        .withColumn(
            "balance_to_income",
            col("avg_monthly_balance")
            / (
                col("annual_income") / 12 + 1
            )
        )
        .withColumn(
            "digital_engagement",
            (
                0.6 * col("digital_txn_ratio")
            )
            +
            (
                0.4
                * (
                    col("mobile_logins_30d") / 101
                )
            )
        )
        .withColumn(
            "channel_engagement",
            col("mobile_logins_30d")
            + col("web_visits_30d")
        )
        .withColumn(
            "product_penetration",
            col("existing_products")
            / (
                col("tenure_months") / 12 + 1
            )
        )
        .withColumn(
            "delinquency_rate",
            col("delinquency_12m") / 12
        )
        .withColumn(
            "contact_pressure",
            col("marketing_contacts_90d") / 3
        )
    )


    # --------------------------------------------------------
    # Business flags
    # --------------------------------------------------------

    print("Creating business flags...")

    df = (
        df
        .withColumn(
            "high_value_customer",
            when(
                (
                    col("annual_income") >= 120000
                )
                &
                (
                    col("avg_monthly_balance") >= 10000
                ),
                1
            ).otherwise(0)
        )
        .withColumn(
            "digital_customer",
            when(
                (
                    col("digital_txn_ratio") >= 0.70
                )
                &
                (
                    col("mobile_logins_30d") >= 15
                ),
                1
            ).otherwise(0)
        )
        .withColumn(
            "credit_risk_flag",
            when(
                (
                    col("credit_score") < 600
                )
                |
                (
                    col("credit_utilization") > 0.80
                )
                |
                (
                    col("delinquency_12m") >= 2
                ),
                1
            ).otherwise(0)
        )
    )


    # --------------------------------------------------------
    # Interaction features
    # --------------------------------------------------------

    print("Creating interaction features...")

    df = (
        df
        .withColumn(
            "income_x_credit_score",
            (
                col("annual_income") / 100000
            )
            * col("credit_score")
        )
        .withColumn(
            "digital_x_spend",
            col("digital_txn_ratio")
            * col("monthly_spend")
        )
        .withColumn(
            "tenure_x_products",
            col("tenure_months")
            * col("existing_products")
        )
        .withColumn(
            TARGET,
            col(TARGET).cast("double")
        )
    )

    return df


# ============================================================
# 5. Main Processing
# ============================================================

try:

    print("================================================")
    print("BANK CUSTOMER ACQUISITION PREPROCESSING")
    print("================================================")

    print("Input path:")
    print(INPUT_PATH)

    print("Output path:")
    print(OUTPUT_PATH)


    # ========================================================
    # Read all Parquet files from S3
    # ========================================================

    print("================================================")
    print("READING RAW DATA")
    print("================================================")

    df = spark.read.parquet(INPUT_PATH)

    input_count = df.count()

    print(
        "Input records before preprocessing:",
        input_count
    )


    # ========================================================
    # Display original columns
    # ========================================================

    print("================================================")
    print("ORIGINAL COLUMNS")
    print("================================================")

    print(df.columns)


    # ========================================================
    # Normalize column names
    # ========================================================

    print("================================================")
    print("NORMALIZING COLUMN NAMES")
    print("================================================")

    df = df.toDF(
        *[
            c.strip().lower()
            for c in df.columns
        ]
    )

    print("Normalized columns:")
    print(df.columns)


    # ========================================================
    # Display schema
    # ========================================================

    print("================================================")
    print("INPUT SCHEMA")
    print("================================================")

    df.printSchema()


    # ========================================================
    # Validate required columns
    # ========================================================

    required_columns = (
        [
            "customer_id",
            "campaign_date",
            TARGET
        ]
        + NUMERIC_FEATURES
        + CATEGORICAL_FEATURES
    )

    missing_columns = [
        c
        for c in required_columns
        if c not in df.columns
    ]


    if missing_columns:

        print("================================================")
        print("MISSING REQUIRED COLUMNS")
        print("================================================")

        print("Missing columns:")
        print(missing_columns)

        print("Available columns:")
        print(df.columns)

        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing_columns)
        )


    # ========================================================
    # Build features
    # ========================================================

    processed = build_features(df)


    # ========================================================
    # Count processed records
    # ========================================================

    processed_count = processed.count()

    print("Processed records:")
    print(processed_count)


    # ========================================================
    # Show processed schema
    # ========================================================

    print("================================================")
    print("PROCESSED SCHEMA")
    print("================================================")

    processed.printSchema()


    # ========================================================
    # Show sample
    # ========================================================

    print("================================================")
    print("SAMPLE PROCESSED RECORDS")
    print("================================================")

    processed.show(
        10,
        truncate=False
    )


    # ========================================================
    # Write processed data
    # ========================================================

    print("================================================")
    print("WRITING PROCESSED DATA")
    print("================================================")

    print("Output path:")
    print(OUTPUT_PATH)
    
    processed = processed.repartition(5)


    (
        processed
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )


    # ========================================================
    # Final summary
    # ========================================================

    print("================================================")
    print("PREPROCESSING COMPLETED SUCCESSFULLY")
    print("================================================")

    print(
        "Input records:",
        input_count
    )

    print(
        "Processed records:",
        processed_count
    )

    print(
        "Output:",
        OUTPUT_PATH
    )


    # ========================================================
    # Commit Glue job
    # ========================================================

    job.commit()


except Exception as e:

    print("================================================")
    print("PREPROCESSING JOB FAILED")
    print("================================================")

    print("Error:")
    print(str(e))

    raise