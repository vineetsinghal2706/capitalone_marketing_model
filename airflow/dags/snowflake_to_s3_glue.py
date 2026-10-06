import pendulum

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.ssm import SsmRunCommandOperator


local_tz = pendulum.timezone("Asia/Kolkata")

AWS_REGION = "us-east-1"
EC2_INSTANCE_ID = "i-06ea3e388448b991c"
ECR_IMAGE = (
    "783495713343.dkr.ecr.us-east-1.amazonaws.com/"
    "bank-marketing-batch-scoring:latest"
)

with DAG(
    dag_id="bank_marketing_end_to_end",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["aws", "glue", "scoring", "mlflow"],
) as dag:

    # -------------------------------------------------------
    # 1. Snowflake → S3 Raw
    # -------------------------------------------------------

    run_snowflake_to_s3 = GlueJobOperator(
        task_id="run_snowflake_to_s3",
        job_name="snowflake-to-s3-bank-marketing",
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--run_date": "{{ ds }}"
        },
    )

    # -------------------------------------------------------
    # 2. S3 Raw → S3 Processed
    # -------------------------------------------------------

    run_preprocessing = GlueJobOperator(
        task_id="run_preprocessing",
        job_name="marketing-model-preprocessing",
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--run_date": "{{ ds }}"
        },
    )

    # -------------------------------------------------------
    # 3. EC2 → Docker → Batch Scoring
    # -------------------------------------------------------

    run_scoring = SsmRunCommandOperator(
        task_id="run_batch_scoring",
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        document_name="AWS-RunShellScript",
        wait_for_completion=True,

        run_command_kwargs={
            "InstanceIds": [EC2_INSTANCE_ID],

            "Parameters": {
                "commands": [
                    # Login to ECR
                    "aws ecr get-login-password --region us-east-1 | "
                    "docker login --username AWS "
                    "--password-stdin "
                    "783495713343.dkr.ecr.us-east-1.amazonaws.com",

                    # Pull latest image
                    f"docker pull {ECR_IMAGE}",

                    # Run scoring
                    f"""docker run --rm \
-e AWS_DEFAULT_REGION=us-east-1 \
-e MLFLOW_TRACKING_URI="arn:aws:sagemaker:us-east-1:783495713343:mlflow-tracking-server/marketing-mlflow-server" \
{ECR_IMAGE} \
--run-date {{{{ ds }}}} \
--input-s3 s3://bank-marketing-ml-poc-vineet/processed/marketing_features/{{{{ ds }}}}/ \
--output-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/{{{{ ds }}}}/ \
--metrics-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/{{{{ ds }}}}/metrics.json"""
                ]
            }
        },
    )

    # -------------------------------------------------------
    # Dependency
    # -------------------------------------------------------

    run_snowflake_to_s3 >> run_preprocessing >> run_scoring
