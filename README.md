# Bank Marketing MLOps POC

An end-to-end AWS-based proof of concept for a bank marketing response model. The pipeline extracts customer data from Snowflake, preprocesses it with AWS Glue, batch-scores the processed data on EC2 using a Dockerized 100-tree IF/ELSE scoring implementation, writes results and metrics to Amazon S3, and logs scoring runs to SageMaker Managed MLflow.

## Architecture

```text
Snowflake
   |
   | Glue Job 1
   v
S3 Raw
raw/<run_date>/
   |
   | Glue Job 2
   v
S3 Processed
processed/marketing_features/<run_date>/
   |
   | Airflow
   | SSM Run Command
   v
EC2
   |
   | docker pull from ECR
   v
Dockerized Batch Scoring
   |
   +---------------------> S3 Scored Output
   |                        output/model_v1/<run_date>/
   |
   +---------------------> S3 Metrics
   |                        output/model_v1/<run_date>/metrics.json
   |
   +---------------------> SageMaker Managed MLflow
                            bank-marketing-scoring
```

The Airflow DAG enforces the dependency:

```text
Snowflake -> S3 Raw -> Preprocessing -> Batch Scoring
```

Each stage uses the same Airflow execution date through `{{ ds }}`, so a run for `2026-10-06` reads and writes the corresponding date-partitioned S3 paths.

## AWS components

| Component | Purpose |
|---|---|
| Snowflake | Source customer data |
| AWS Glue Job 1 | Extract Snowflake data and write raw Parquet to S3 |
| Amazon S3 | Raw, processed, scored-data and metrics storage |
| AWS Glue Job 2 | Feature engineering and preprocessing |
| Apache Airflow | End-to-end orchestration |
| AWS Systems Manager | Execute the scoring container on EC2 |
| Amazon EC2 | Batch scoring compute |
| Amazon ECR | Store the scoring Docker image |
| SageMaker Managed MLflow | Track scoring runs, parameters, metrics and artifacts |
| GitHub Actions | Python/DAG validation, Glue deployment and scoring-image CI/CD |

## Repository structure

```text
capitalone_marketing_model/
├── .github/
│   └── workflows/
│       ├── ci-cd.yml
│       └── scoring.yml
├── airflow/
│   └── dags/
│       └── snowflake_to_s3_glue.py
├── glue/
│   ├── snowflake_to_s3/
│   │   └── snowflake_to_s3.py
│   └── preprocessing/
│       └── marketing-model-preprocessing.py
├── scoring/
│   └── score_if_else.py
├── batch_score.py
├── Dockerfile
├── requirements.txt
├── tests/
│   ├── test_glue_job1.py
│   └── test_glue_job2.py
├── .gitignore
└── README.md
```

## 1. Glue Job 1 — Snowflake to S3

**Script:** `glue/snowflake_to_s3/snowflake_to_s3.py`

The job:

1. Connects to the configured AWS Glue Snowflake connection.
2. Reads `INGEST_DATA.PUBLIC.CUSTOMERS`.
3. Filters records where `CUSTOMER_ID IS NOT NULL`.
4. Prints the schema, sample records and extracted record count.
5. Repartitions the data into 5 partitions.
6. Writes Parquet to:

```text
s3://bank-marketing-ml-poc-vineet/raw/<run_date>/
```

The `run_date` argument is supplied by Airflow.

### Required Glue configuration

The Glue job is expected to have a Snowflake connection named:

```text
Snowflake connection
```

The current code uses:

```text
Database   : INGEST_DATA
Schema     : PUBLIC
Warehouse  : COMPUTE_WH
Table      : CUSTOMERS
```

## 2. Glue Job 2 — Preprocessing

**Script:** `glue/preprocessing/marketing-model-preprocessing.py`

Input:

```text
s3://bank-marketing-ml-poc-vineet/raw/<run_date>/
```

Output:

```text
s3://bank-marketing-ml-poc-vineet/processed/marketing_features/<run_date>/
```

The preprocessing pipeline:

- Removes duplicate customers using `customer_id`.
- Normalizes column names to lowercase.
- Converts `campaign_date` to a date.
- Cleans categorical fields using trim/lower.
- Creates month, quarter, day-of-week, weekend and cyclic month features.
- Applies business-rule outlier caps to income, spend and balance.
- Creates logarithmic features.
- Creates spend-to-income and balance-to-income ratios.
- Creates digital/customer engagement features.
- Creates product penetration and delinquency/contact-pressure features.
- Creates business flags such as `high_value_customer`, `digital_customer` and `credit_risk_flag`.
- Creates interaction features.
- Validates required columns before processing.
- Repartitions the output into 5 partitions.
- Writes Parquet using overwrite mode.

The current target column is:

```text
campaign_response
```

## 3. Batch scoring

**Entrypoint:** `batch_score.py`

**Model implementation:** `scoring/score_if_else.py`

The scoring implementation contains a 100-tree IF/ELSE representation of the model. It calculates:

- prediction margin
- probability
- binary prediction
- 3-digit score

The current POC score mapping is:

```text
score = clip(round(100 + 899 * probability), 100, 999)
```

This mapping is explicitly marked in the scoring code as a POC mapping and should be replaced by the modeling-team-approved scorecard mapping before production use.

### Batch scoring inputs

```bash
python batch_score.py   --run-date 2026-10-06   --input-s3 s3://bank-marketing-ml-poc-vineet/processed/marketing_features/2026-10-06/   --output-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-06/   --metrics-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-06/metrics.json
```

Optional target column:

```bash
--target-column campaign_response
```

The batch scorer:

1. Lists all Parquet files under the input S3 prefix.
2. Reads the files into pandas.
3. Concatenates the input data.
4. Scores every row with the IF/ELSE model.
5. Writes scored data to:

```text
<output-s3>/scored_data.parquet
```

6. Writes metrics to the supplied `metrics.json` path.
7. Optionally creates an MLflow run when `MLFLOW_TRACKING_URI` is configured.

## 4. Scoring metrics

Operational metrics include:

```text
row_count
score_min
score_max
score_mean
score_median
probability_min
probability_max
probability_mean
prediction_rate
```

When the target column is available and contains non-null values, the scorer also calculates:

```text
accuracy
precision
recall
f1
roc_auc
log_loss
actual_response_rate
confusion_matrix
```

This means the same batch execution produces both scored customer data and an evaluation/monitoring payload.

## 5. SageMaker Managed MLflow

The scoring application supports SageMaker Managed MLflow through:

```text
MLFLOW_TRACKING_URI
```

Example:

```text
arn:aws:sagemaker:us-east-1:783495713343:mlflow-tracking-server/marketing-mlflow-server
```

Default experiment:

```text
bank-marketing-scoring
```

The MLflow run records model metadata such as:

```text
model_name       = MarketingResponseGBM
model_version    = 1.0
model_type       = XGBoost
number_of_trees  = 100
scoring_method   = IF_ELSE
run_date
input_s3
output_s3
metrics_s3
```

Numeric operational and model-performance metrics are logged, and the metrics JSON plus a scoring summary are stored as MLflow artifacts.

If `MLFLOW_TRACKING_URI` is not configured, scoring continues and the S3 outputs are still written; MLflow logging is disabled.

## 6. Airflow orchestration

**DAG:** `bank_marketing_end_to_end`

**File:** `airflow/dags/snowflake_to_s3_glue.py`

The DAG currently has:

```python
schedule=None
catchup=False
start_date=2026-10-01 (Asia/Kolkata)
```

Therefore, it is currently designed for manual triggering from Airflow.

### Tasks

| Task | Technology | Purpose |
|---|---|---|
| `run_snowflake_to_s3` | Glue | Snowflake → S3 Raw |
| `run_preprocessing` | Glue | S3 Raw → S3 Processed |
| `run_batch_scoring` | SSM / EC2 | Pull ECR image and run batch scoring |

The dependency is:

```text
run_snowflake_to_s3
        |
        v
run_preprocessing
        |
        v
run_batch_scoring
```

Both Glue tasks wait for completion before the next task can start, and the scoring task runs only after preprocessing succeeds.

### Scoring execution on EC2

Airflow uses AWS Systems Manager Run Command to:

1. Authenticate Docker to ECR.
2. Pull the latest scoring image.
3. Run the container.
4. Pass the Airflow execution date to `batch_score.py`.
5. Pass the processed-data, output and metrics S3 paths.
6. Set the SageMaker MLflow Tracking Server ARN.

Current ECR image:

```text
783495713343.dkr.ecr.us-east-1.amazonaws.com/bank-marketing-batch-scoring:latest
```

## 7. Docker image

**Dockerfile:** `Dockerfile`

The image is based on:

```text
python:3.11-slim
```

It installs `requirements.txt`, copies the scoring implementation and uses:

```text
python batch_score.py
```

as its entrypoint.

Build locally:

```bash
docker build -t bank-marketing-batch-scoring .
```

Run locally with example arguments:

```bash
docker run --rm   -e AWS_DEFAULT_REGION=us-east-1   -e MLFLOW_TRACKING_URI="<SAGEMAKER_MLFLOW_TRACKING_SERVER_ARN>"   bank-marketing-batch-scoring   --run-date 2026-10-06   --input-s3 s3://bank-marketing-ml-poc-vineet/processed/marketing_features/2026-10-06/   --output-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-06/   --metrics-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-06/metrics.json
```

The runtime identity must have access to the relevant S3 prefixes and to the MLflow tracking server when MLflow logging is enabled.

## 8. Amazon ECR and GitHub Actions

### Glue CI/CD workflow

**Workflow:** `.github/workflows/ci-cd.yml`

Triggered by pushes to:

```text
main
develop
```

and by pull requests to `main`.

It:

1. Validates Python syntax for the Glue and Airflow code.
2. Installs Airflow/provider dependencies and validates the DAG.
3. On push, assumes an AWS IAM role using GitHub OIDC.
4. Uploads Glue Job 1 and Glue Job 2 scripts to the configured S3 code bucket.
5. Verifies the deployed files.

Required GitHub repository variables:

```text
AWS_DEPLOY_ROLE_ARN
GLUE_CODE_BUCKET
```

### Scoring-image CI/CD workflow

**Workflow:** `.github/workflows/scoring.yml`

On push to `main`, when the scoring-related files change, it:

1. Assumes the AWS deployment role using GitHub OIDC.
2. Logs in to Amazon ECR.
3. Builds a Linux/AMD64 Docker image.
4. Pushes an immutable image tagged with the Git commit SHA.
5. Updates the `latest` tag.

ECR repository:

```text
bank-marketing-batch-scoring
```

### Important CI/CD separation

```text
GitHub Actions
    |
    +--> Validate code
    +--> Deploy Glue scripts to S3
    +--> Build/push scoring image to ECR

Airflow
    |
    +--> Run Glue Job 1
    +--> Run Glue Job 2
    +--> Trigger EC2 scoring via SSM
```

GitHub Actions does not run the local Airflow instance.

## 9. Running Airflow locally

Airflow can be run separately from the repository's CI/CD workflows.

The DAG file should be placed in the Airflow DAG directory:

```text
airflow/dags/snowflake_to_s3_glue.py
```

The current DAG is manually triggered because `schedule=None`.

Before triggering it, confirm:

- the DAG is visible and has no import errors;
- the AWS Airflow connection `aws_default` is configured;
- the two AWS Glue jobs exist with the expected names;
- the EC2 instance is online and managed by SSM;
- Docker is installed and the EC2 instance can pull from ECR;
- the scoring image `latest` exists in ECR;
- the EC2 IAM role has the required S3/ECR/MLflow permissions.

## 10. S3 layout

The expected S3 layout for a run date is:

```text
s3://bank-marketing-ml-poc-vineet/
├── raw/
│   └── <run_date>/
│       └── *.parquet
├── processed/
│   └── marketing_features/
│       └── <run_date>/
│           └── *.parquet
└── output/
    └── model_v1/
        └── <run_date>/
            ├── scored_data.parquet
            └── metrics.json
```

## 11. Testing and validation

The repository contains tests for the Glue components:

```text
tests/test_glue_job1.py
tests/test_glue_job2.py
```

Basic Python validation:

```bash
python -m compileall -q glue airflow
python -m py_compile airflow/dags/snowflake_to_s3_glue.py
```

Run the test suite:

```bash
pytest
```

## 12. Security and configuration

Do not commit:

- AWS access keys or secret keys.
- Snowflake usernames/passwords.
- MLflow credentials.
- Other production secrets.

Use AWS IAM roles, GitHub OIDC, GitHub repository variables/secrets and AWS-native connection/secret mechanisms.

The current CI/CD design uses GitHub OIDC rather than long-lived AWS access keys.

## 13. Current scope and limitations

This repository is currently an end-to-end POC for data ingestion, preprocessing, batch scoring, orchestration and tracking.

Not currently included:

- SageMaker model training jobs.
- Automated model retraining.
- Production model promotion/approval workflows.
- Production-grade score calibration.
- Real-time scoring API.
- Automated Airflow deployment.

The current scoring function uses an IF/ELSE representation of the 100-tree model, and the 100–999 score mapping is a POC mapping pending modeling-team approval.

## End-to-end execution

For a manual run of date `2026-10-06`:

```text
1. Airflow triggers bank_marketing_end_to_end
2. Glue Job 1 extracts Snowflake data
3. Raw Parquet is written to S3 raw/2026-10-06/
4. Glue Job 2 reads the raw partition
5. Preprocessed Parquet is written to processed/marketing_features/2026-10-06/
6. Airflow triggers SSM on EC2
7. EC2 authenticates to ECR
8. EC2 pulls bank-marketing-batch-scoring:latest
9. Docker runs batch_score.py
10. Scored data is written to output/model_v1/2026-10-06/scored_data.parquet
11. Metrics are written to output/model_v1/2026-10-06/metrics.json
12. A scoring run is logged to SageMaker Managed MLflow when configured
```
