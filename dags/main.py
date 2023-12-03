from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from conf import config

with DAG(dag_id="test", start_date=days_ago(n=0), schedule=timedelta(days=1)) as dag:
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task
    def airflow():
        print("airflow")

    test_job = EmrServerlessStartJobOperator(
        task_id="test_submit_emr_serverless_job",
        aws_conn_id=config.AWS_CONNECTION,
        application_id=config.EMR_APPLICATION_ID,
        execution_role_arn=config.EMR_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://nyc-trip-directory-bucket/code/main.py",
            }
        },
        configuration_overrides={},
    )

    hello >> airflow() >> test_job
