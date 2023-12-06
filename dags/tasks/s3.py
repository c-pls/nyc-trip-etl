import os

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from conf import config

AWS_CONN_ID = config.AWS_CONN_ID


def upload_file_to_s3(bucket_name: str, object_key: str, file_path: str) -> str:
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.load_file(
        filename=file_path,
        key=object_key,
        bucket_name=bucket_name,
        replace=True,
    )

    s3_uri = f"s3://{bucket_name}/{object_key}"
    return s3_uri


@task(task_id="upload_raw_data_to_s3")
def upload_raw_data_to_s3(bucket_name: str, object_key: str, file_path: str):
    try:
        s3_uri = upload_file_to_s3(bucket_name, object_key, file_path)
        return s3_uri
    finally:
        os.remove(file_path)


@task(task_id="upload_script_to_s3")
def upload_script_to_s3(bucket_name: str, object_key: str, file_path: str):
    upload_file_to_s3(bucket_name, object_key, file_path)


@task(task_id="delete_raw_data")
def delete_raw_data(s3_uri: str):
    def parse_s3_uri(s3_uri: str) -> (str, str):
        """
        Parse S3 URI and extract bucket name and object key.

        Args: s3_uri (str): S3 URI, e.g., s3://bucket-name/path/to/object

        Returns: tuple: (bucket_name, object_key)
        """

        from urllib.parse import urlparse

        parsed_uri = urlparse(s3_uri)

        # Check if the scheme is 's3'
        if parsed_uri.scheme != "s3":
            raise ValueError(
                f"Invalid S3 URI. Scheme must be 's3', got: {parsed_uri.scheme}"
            )

        # Extract bucket name and object key
        bucket_name = parsed_uri.netloc
        object_key = parsed_uri.path.lstrip("/")

        return bucket_name, object_key

    bucket, object_key = parse_s3_uri(s3_uri)

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.delete_objects(bucket=bucket, keys=object_key)
