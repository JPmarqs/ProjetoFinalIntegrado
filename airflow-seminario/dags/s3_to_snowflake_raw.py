"""Carrega o snapshot Parquet do S3 efemero no Snowflake e executa o dbt.

As credenciais AWS temporarias existem somente no ambiente do container e no
stage temporario da sessao Snowflake. Nenhum segredo e enviado ao XCom ou log.
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import PurePosixPath

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from pipeline_constants import RAW_COLUMNS


LOGGER = logging.getLogger(__name__)
SNOWFLAKE_CONN_ID = "snowflake_default"


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"A variavel de ambiente {name} nao foi configurada.")
    return value


def sql_identifier(name: str) -> str:
    normalized = name.strip().upper()
    if not normalized.replace("_", "").isalnum():
        raise ValueError(f"Identificador Snowflake invalido: {name!r}")
    return normalized


@dag(
    dag_id="s3_to_snowflake_raw",
    description="Carrega o Parquet do S3 em RAW.SINISTROS e executa o dbt staging",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "projeto_final_ia",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["s3", "parquet", "snowflake", "dbt", "raw"],
)
def s3_to_snowflake_raw():
    @task
    def validate_s3_source() -> dict[str, str | int]:
        import boto3

        bucket = required_env("S3_BUCKET_NAME")
        prefix = os.getenv("S3_PREFIX", "raw/airflow").strip().strip("/")
        filename = required_env("PARQUET_FILENAME")
        if not filename.lower().endswith(".parquet"):
            raise ValueError("PARQUET_FILENAME deve possuir a extensao .parquet.")
        object_key = str(PurePosixPath(prefix) / filename) if prefix else filename

        response = boto3.client("s3").head_object(Bucket=bucket, Key=object_key)
        size_bytes = int(response["ContentLength"])
        content_type = response.get("ContentType", "")
        if size_bytes <= 0:
            raise RuntimeError(f"O objeto s3://{bucket}/{object_key} esta vazio.")
        if content_type != "application/vnd.apache.parquet":
            raise RuntimeError(
                "O objeto S3 nao possui o Content-Type esperado para Parquet: "
                f"{content_type!r}."
            )

        result = {
            "bucket": bucket,
            "prefix": prefix,
            "filename": filename,
            "object_key": object_key,
            "size_bytes": size_bytes,
            "content_type": content_type,
            "etag": response.get("ETag", "").strip('"'),
        }
        LOGGER.info(
            "Fonte S3 validada: s3://%s/%s (%s bytes)",
            bucket,
            object_key,
            size_bytes,
        )
        return result

    @task
    def load_raw_snapshot(source: dict[str, str | int]) -> dict[str, int | str]:
        database = sql_identifier(required_env("SNOWFLAKE_DATABASE"))
        access_key = required_env("AWS_ACCESS_KEY_ID")
        secret_key = required_env("AWS_SECRET_ACCESS_KEY")
        session_token = required_env("AWS_SESSION_TOKEN")

        bucket = str(source["bucket"])
        prefix = str(source["prefix"])
        filename = str(source["filename"])
        stage_url = f"s3://{bucket}/{prefix}/" if prefix else f"s3://{bucket}/"

        raw_schema = f"{database}.RAW"
        file_format = f"{raw_schema}.PRF_PARQUET_FORMAT"
        stage = f"{raw_schema}.AWS_LAB_S3_STAGE"
        next_table = f"{raw_schema}.SINISTROS_NEXT"
        target_table = f"{raw_schema}.SINISTROS"

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        connection = hook.get_conn()
        cursor = connection.cursor()

        try:
            cursor.execute(
                f"""
                CREATE OR REPLACE FILE FORMAT {file_format}
                    TYPE = PARQUET
                    USE_LOGICAL_TYPE = TRUE
                    BINARY_AS_TEXT = FALSE
                    USE_VECTORIZED_SCANNER = TRUE
                """
            )

            cursor.execute(
                f"""
                CREATE OR REPLACE TEMPORARY STAGE {stage}
                    URL = %s
                    CREDENTIALS = (
                        AWS_KEY_ID = %s
                        AWS_SECRET_KEY = %s
                        AWS_TOKEN = %s
                    )
                    FILE_FORMAT = {file_format}
                """,
                (stage_url, access_key, secret_key, session_token),
            )

            column_definitions = ",\n".join(
                f"{column} VARCHAR" for column in RAW_COLUMNS
            )
            cursor.execute(
                f"""
                CREATE OR REPLACE TRANSIENT TABLE {next_table} (
                    {column_definitions},
                    SOURCE_FILE VARCHAR,
                    FILE_LAST_MODIFIED TIMESTAMP_TZ,
                    LOADED_AT TIMESTAMP_TZ
                )
                """
            )

            cursor.execute(
                f"""
                COPY INTO {next_table}
                FROM @{stage}
                FILES = (%s)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                INCLUDE_METADATA = (
                    SOURCE_FILE = METADATA$FILENAME,
                    FILE_LAST_MODIFIED = METADATA$FILE_LAST_MODIFIED,
                    LOADED_AT = METADATA$START_SCAN_TIME
                )
                FORCE = TRUE
                ON_ERROR = 'ABORT_STATEMENT'
                """,
                (filename,),
            )
            copy_result = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM {next_table}")
            row_count = int(cursor.fetchone()[0])
            if row_count <= 0:
                raise RuntimeError("A carga Snowflake terminou sem nenhuma linha.")

            cursor.execute(
                f"CREATE OR REPLACE TRANSIENT TABLE {target_table} CLONE {next_table}"
            )
            cursor.execute(f"DROP TABLE IF EXISTS {next_table}")

            result = {
                "table": target_table,
                "rows_loaded": row_count,
                "copy_status": str(copy_result[1]) if copy_result else "UNKNOWN",
            }
            LOGGER.info("Snapshot RAW publicado: %s", result)
            return result
        finally:
            try:
                cursor.execute(f"DROP STAGE IF EXISTS {stage}")
            except Exception:
                LOGGER.warning("Nao foi possivel remover o stage temporario.", exc_info=True)
            cursor.close()
            connection.close()

    source = validate_s3_source()
    raw_load = load_raw_snapshot(source)

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            "dbt build --select staging "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt "
            "--target dev"
        ),
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt"},
        append_env=True,
    )

    validate_staging = SQLExecuteQueryOperator(
        task_id="validate_staging",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT
                COUNT(*) AS row_count,
                COUNT_IF(cd_bat IS NULL) AS null_cd_bat,
                COUNT_IF(latitude IS NOT NULL AND longitude IS NOT NULL)
                    AS rows_with_coordinates
            FROM STAGING.STG_SINISTROS
        """,
        do_xcom_push=True,
    )

    source >> raw_load >> run_dbt_staging >> validate_staging


s3_to_snowflake_raw()
