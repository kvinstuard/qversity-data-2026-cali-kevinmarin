from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

with DAG(
    dag_id="dbt_transformations",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:
    
  dbt_run = DockerOperator(
    task_id="dbt_run",
    image="mi_dbt_image",
    command="dbt run --project-dir /app/dbt --profiles-dir /app/dbt",
    network_mode="qversity_default",
    auto_remove=True,
    mount_tmp_dir=False,
    docker_url="unix://var/run/docker.sock",
)

  dbt_test = DockerOperator(
    task_id="dbt_test",
    image="mi_dbt_image",
    command="dbt test --project-dir /app/dbt --profiles-dir /app/dbt",
    network_mode="qversity_default",
    auto_remove=True,
    mount_tmp_dir=False,
    docker_url="unix://var/run/docker.sock",
)


dbt_run >> dbt_test