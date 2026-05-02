from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import requests


def create_raw_trips_table():

    CLICKHOUSE_HOST = "clickhouse"
    CLICKHOUSE_PORT = 8123
    CLICKHOUSE_USER = "default"
    CLICKHOUSE_PASSWORD = "default"
    CLICKHOUSE_DATABASE = "default"

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"

    query = """
    CREATE TABLE IF NOT EXISTS raw_trips
    (
        tripduration UInt32,

        starttime DateTime,
        stoptime DateTime,

        start_station_id UInt32,
        start_station_name String,
        start_station_latitude Float64,
        start_station_longitude Float64,

        end_station_id UInt32,
        end_station_name String,
        end_station_latitude Float64,
        end_station_longitude Float64,

        bikeid UInt32,
        usertype String,
        birth_year Nullable(UInt16),
        gender Nullable(UInt8)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(starttime)
    ORDER BY (starttime);
    """

    params = {
        "query": query,
        "database": CLICKHOUSE_DATABASE,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
    }

    r = requests.post(url, params=params, timeout=30)

    if r.status_code != 200:
        raise Exception(f"ClickHouse error: {r.text}")


default_args = {"owner": "ahmed", "retries": 3, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="trigger_dag",
    default_args=default_args,
    description="A DAG to trigger spark job and dbt transformations",
    start_date=datetime(2020, 10, 10),
    schedule=None,
) as dag:
    create_table = PythonOperator(
        task_id="create_raw_trips_table",
        python_callable=create_raw_trips_table,
    )

    spark_job = DockerOperator(
        task_id="spark_job",
        image="spark:latest",
        command="spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/jobs/ingest_job.py",
        network_mode="bikespark_bikespark-net",
        docker_url="tcp://var/run/docker.sock",
        auto_remove="success",
    )

    run_dbt = DockerOperator(
        task_id="run_dbt",
        image="dbt-clickhouse:latest",
        command="run",
        working_dir="/usr/app/citibike_project",
        network_mode="bikespark_bikespark-net",
        docker_url="tcp://var/run/docker.sock",
        auto_remove="success",
    )

    create_table  # >> spark_job >> run_dbt
