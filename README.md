# Bank Marketing MLOps POC

dCurrent scope:

Snowflake -> Glue Job 1 -> S3 Raw -> Glue Job 2 -> S3 Processed

Airflow orchestrates the two Glue jobs. SageMaker training is intentionally not included.

## Structure

```text
bank-marketing-mlops/
├── glue/
│   ├── snowflake_to_s3/
│   │   └── snowflake_to_s3.py
│   └── preprocessing/
│       └── marketing-model-preprocessing.py
├── airflow/
│   └── dags/
│       └── snowflake_to_s3_glue.py
├── .github/
│   └── workflows/
│       └── ci-cd.yml
├── tests/
├── requirements.txt
├── .gitignore
└── README.md
```

## Data flow

Glue Job 1 writes:

`s3://bank-marketing-ml-poc-vineet/raw/<run_date>/`

Glue Job 2 reads that path and writes:

`s3://bank-marketing-ml-poc-vineet/processed/marketing_features/<run_date>/`

Both jobs receive the same Airflow logical date through:

```python
"--run_date": "{{ ds }}"
```

## Airflow

DAG ID:

`snowflake_to_s3_glue`

Current schedule is `None`, so it can be manually triggered.

For Monday 2:30 PM IST, change to:

```python
schedule="30 14 * * 1"
```

Dependency:

```text
Glue Job 1 >> Glue Job 2
```

## GitHub Actions

GitHub Actions:

1. Validates Python syntax.
2. Validates the DAG file.
3. On push, uploads both Glue scripts to an S3 code bucket.

It does NOT run your local Airflow Docker instance.

Your local Airflow must remain running separately.

## Required GitHub repository variables

Create:

```text
AWS_DEPLOY_ROLE_ARN
GLUE_CODE_BUCKET
```

The workflow uses GitHub OIDC to assume the AWS IAM role. Do not commit AWS keys or Snowflake passwords.

## Existing AWS Glue jobs

The DAG expects:

```text
snowflake-to-s3-bank-marketing
marketing-model-preprocessing
```

Configure each Glue job's Script location to the corresponding S3 code path deployed by GitHub Actions.

## IAM

The GitHub deployment role needs permission to upload the two scripts to the code bucket.

The Airflow AWS identity needs Glue permissions such as:

```text
glue:GetJob
glue:StartJobRun
glue:GetJobRun
glue:GetJobRuns
```

plus any S3 permissions required by the Glue jobs.

## Important separation

GitHub Actions = CI/CD

Airflow = orchestration/scheduling

```text
GitHub push
   |
   v
GitHub Actions
   |
   +-- validate
   +-- deploy Glue scripts
   |
   v
Airflow
   |
   +-- Glue Job 1
   |
   +-- Glue Job 2
```

SageMaker/MLflow will be added later.
