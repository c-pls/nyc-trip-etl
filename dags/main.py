from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from conf import config
from tasks.emr import submit_emr_serverless_job
from tasks.pull_raw_data import pull_raw_data
from tasks.s3 import delete_raw_data, upload_raw_data_to_s3, upload_script_to_s3
from tasks.sf import (
    create_external_stage_snowflake_s3,
    load_to_table,
    truncate_table,
)

TRIP_TYPE_LIST = ["green", "fhvhv"]


default_args = {
    "owner": "Chinh Pham",
    "retries": 1,
    "retry_delay": timedelta(seconds=0),
    "depends_on_past": False,
    "email_on_failure": False,
}


@task_group(group_id="ETL")  # type: ignore
def pipeline():
    def build_group(trip_type: str):
        @task_group(group_id=f"{trip_type}_ETL")
        def etl():
            raw_output_file_path = pull_raw_data(trip_type=trip_type)
            upload_script_task = upload_script_to_s3(
                bucket_name=config.S3_BUCKET_NAME,
                object_key=f"{config.SCRIPT_FOLDER}/{trip_type}_trip/transform.py",
                file_path=f"{config.PROJECT_ROOT}/spark/{trip_type}_trip/transform.py",
            )

            raw_data_uri = upload_raw_data_to_s3(
                bucket_name=config.S3_BUCKET_NAME,
                object_key=f"etl/raw_data/{pendulum.now().format('YYYY/MM/DD')}/{trip_type}/raw_data.parquet",
                file_path=raw_output_file_path,
            )

            emr_job_task = submit_emr_serverless_job(
                trip_type=trip_type,
                entry_point=f"s3://{config.S3_BUCKET_NAME}/{config.SCRIPT_FOLDER}/{trip_type}_trip/transform.py",
                entry_point_arguments=[
                    raw_data_uri,
                    f"s3://{config.S3_BUCKET_NAME}/{config.OUTPUT_FOLDER}/{trip_type}_trip",
                ],
            )

            delete_task = delete_raw_data(raw_data_uri)

            truncate_staging_table = truncate_table(
                task_id="truncate_raw_table",
                database=config.SNOWFLAKE_DATABASE,
                schema=config.SNOWFLAKE_STAGING_SCHEMA,
                table_name=config.SNOWFLAKE_TABLE[trip_type],
            )
            create_external_stage = create_external_stage_snowflake_s3(
                trip_type=trip_type,
                database=config.SNOWFLAKE_DATABASE,
                schema=config.SNOWFLAKE_STAGING_SCHEMA,
                s3_uri=f"s3://{config.S3_BUCKET_NAME}/{config.OUTPUT_FOLDER}/green_trip",
                file_format="(type = parquet)",
            )

            load_data_to_table = load_to_table(
                trip_type=trip_type,
                database=config.SNOWFLAKE_DATABASE,
                schema=config.SNOWFLAKE_STAGING_SCHEMA,
                table_name=config.SNOWFLAKE_TABLE[trip_type],
                stage_name=create_external_stage,
            )

            chain(
                raw_output_file_path,
                [raw_data_uri, upload_script_task],
                emr_job_task,
                delete_task,
                [truncate_staging_table, create_external_stage],
                load_data_to_table,
            )

        return etl()

    return list(map(build_group, TRIP_TYPE_LIST))


with DAG(
    dag_id="NYC_TRIP_ETL",
    default_args=default_args,
    start_date=days_ago(n=0),
    schedule=None,
    catchup=False,
    template_searchpath="include",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> pipeline() >> end
