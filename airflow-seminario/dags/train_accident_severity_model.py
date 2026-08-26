"""Treina o classificador de acidentes e persiste resultados no S3/Snowflake."""

from datetime import datetime, timedelta
import sys

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="train_accident_severity_model",
    description="Treina Random Forest com dados Snowflake e features de mapas do S3",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "projeto_final_ia",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["snowflake", "s3", "machine-learning"],
)
def train_accident_severity_model():
    @task(execution_timeout=timedelta(hours=2))
    def train_and_persist() -> dict:
        sys.path.insert(0, "/opt/airflow/model")
        from pipeline_integrado import run_training_pipeline

        return run_training_pipeline()

    training = train_and_persist()
    build_marts = BashOperator(
        task_id="build_analytics_marts",
        bash_command=(
            "dbt build --select +marts "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt --target dev"
        ),
    )
    training >> build_marts


train_accident_severity_model()
