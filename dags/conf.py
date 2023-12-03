from dataclasses import dataclass


@dataclass
class Config:
    AWS_CONNECTION = "aws_conn"

    EMR_APPLICATION_ID = "00ff75nttki5vq25"

    EMR_EXECUTION_ROLE_ARN = (
        "arn:aws:iam::434545458459:role/emr-serverless-execution-role"
    )


config = Config()
