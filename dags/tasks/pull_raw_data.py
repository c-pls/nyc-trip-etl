import asyncio
import os

import aiohttp
from airflow.decorators import task
from conf import config


@task(task_id="pull_raw_data")
def pull_raw_data(trip_type: str):
    url = f"{config.BASE_DATASOURCE_URL}/{trip_type}_tripdata_2023-01.parquet"

    folder_path = os.path.join(config.PROJECT_ROOT, "raw_data")

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = os.path.join(folder_path, f"{trip_type}_tripdata_2023-03.parquet")

    async def get_raw_data():
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    with open(file_path, "wb") as f:
                        f.write(content)

                else:
                    raise Exception(
                        f"Request failed with status code {response.status}"
                    )

    asyncio.run(get_raw_data())
    return file_path
