import pendulum

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


local_tz = pendulum.timezone("Asia/Kolkata")


with DAG(
    dag_id="snowflake_to_s3_glue",
    start_date=pendulum.datetime(
        2026,
        10,
        1,
        tz=local_tz
    ),
    schedule=None,
    catchup=False,
    tags=[
        "aws",
        "glue",
        "snowflake",
        "s3",
        "preprocessing"
    ],
) as dag:

    run_snowflake_to_s3 = GlueJobOperator(
        task_id="run_snowflake_to_s3",
        job_name="snowflake-to-s3-bank-marketing",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        script_args={
            "--run_date": "{{ ds }}"
        },
    )

    run_preprocessing = GlueJobOperator(
        task_id="run_preprocessing",
        job_name="marketing-model-preprocessing",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        script_args={
            "--run_date": "{{ ds }}"
        },
    )

    run_snowflake_to_s3 >> run_preprocessing
