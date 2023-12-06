import os
from dataclasses import dataclass


@dataclass
class Config:
    PROJECT_ROOT = os.getenv("AIRFLOW_HOME")

    BASE_DATASOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    AWS_CONN_ID = "aws_conn"

    S3_BUCKET_NAME = "de-666ne92p"

    EMR_APPLICATION_ID = "00ff75nttki5vq25"

    EMR_EXECUTION_ROLE_ARN = (
        "arn:aws:iam::434545458459:role/emr-serverless-execution-role"
    )

    SCRIPT_FOLDER = "etl/spark/script"

    OUTPUT_FOLDER = "etl/spark/output"

    SNOWFLAKE_CONN_ID = "snowflake_conn"

    SNOWFLAKE_AWS_INTEGRATION = "AWS_INTEGRATION"

    SNOWFLAKE_DATABASE = "NYC_TRIP"

    SNOWFLAKE_STAGING_SCHEMA = "STAGING"

    SNOWFLAKE_PRODUCTION_SCHEMA = "PROD"

    SNOWFLAKE_TABLE = {
        "green": "FACT_GREEN_TRIP",
        "fhvhv": "FACT_FHVHV_TRIP",
    }


config = Config()
