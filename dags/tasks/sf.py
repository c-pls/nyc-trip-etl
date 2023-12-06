from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from conf import config


def truncate_table(task_id: str, database: str, schema: str, table_name: str):
    truncate_snowflake_raw_table = SnowflakeOperator(
        task_id=task_id,
        snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
        database=database,
        schema=schema,
        sql="sql/common/truncate_table.sql",
        params={"table_name": table_name},
    )

    return truncate_snowflake_raw_table


@task(task_id="create_external_stage")
def create_external_stage_snowflake_s3(
    trip_type: str, database: str, schema: str, s3_uri: str, file_format: str
) -> str:
    with open(
        f"{config.PROJECT_ROOT}/include/sql/common/snowflake_external_stage_s3.sql", "r"
    ) as f:
        stage_name = f"{trip_type}_stage"
        sql = f.read().format(
            stage_name=stage_name,
            s3_uri=s3_uri,
            storage_integration=config.SNOWFLAKE_AWS_INTEGRATION,
            file_format=file_format,
        )

        sf_hook = SnowflakeHook(
            snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
            database=database,
            schema=schema,
        )
        sf_hook.run(sql=sql)

        return stage_name


@task(task_id="load_to_table")
def load_to_table(
    trip_type: str, database: str, schema: str, table_name: str, stage_name: str
):
    with open(
        f"{config.PROJECT_ROOT}/include/sql/trip/{trip_type}_trip/copy_command.sql", "r"
    ) as f:
        sql = f.read().format(dest_table_name=table_name, stage_name=stage_name)

        sf_hook = SnowflakeHook(
            snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
            database=database,
            schema=schema,
        )
        sf_hook.run(sql=sql)
