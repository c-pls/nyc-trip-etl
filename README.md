# airflow-docker

## Installation

Step 1:

```bash
sudo chmod -R 777 *
```

Step 2:

```bash
docker compose up -d --force-recreate --scale spark-worker=3
```

Step 3: Open [http://localhost:8080/](http://localhost:8080/).

Username: airflow

Password: airflow
