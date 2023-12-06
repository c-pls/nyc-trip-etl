from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from conf import config


def submit_emr_serverless_job(
    trip_type: str,
    entry_point: str,
    entry_point_arguments: list = [],
):
    submit_job = EmrServerlessStartJobOperator(
        task_id=f"submit_emr_serverless_job_{trip_type}",
        aws_conn_id=config.AWS_CONN_ID,
        application_id=config.EMR_APPLICATION_ID,
        execution_role_arn=config.EMR_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": entry_point_arguments,
            }
        },
        configuration_overrides={},
    )
    return submit_job
