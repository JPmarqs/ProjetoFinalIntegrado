"""Valida as conexões do Airflow e do dbt com o Snowflake.

Esta DAG é somente de diagnóstico: não cria, altera ou remove dados.
"""

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


with DAG(
    dag_id="snowflake_dbt_diagnostic",
    description="Testa a autenticação do Airflow e do dbt no Snowflake",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "projeto_final_ia",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["snowflake", "dbt", "diagnostico"],
) as dag:
    test_snowflake_connection = SQLExecuteQueryOperator(
        task_id="test_snowflake_connection",
        conn_id="snowflake_default",
        sql="""
            SELECT
                CURRENT_ACCOUNT() AS account_name,
                CURRENT_USER() AS user_name,
                CURRENT_ROLE() AS role_name,
                CURRENT_WAREHOUSE() AS warehouse_name,
                CURRENT_DATABASE() AS database_name,
                CURRENT_SCHEMA() AS schema_name
        """,
        do_xcom_push=True,
    )

    test_dbt_connection = BashOperator(
        task_id="test_dbt_connection",
        bash_command=(
            "dbt debug "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt "
            "--target dev"
        ),
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt"},
        append_env=True,
    )

    test_snowflake_connection >> test_dbt_connection

