"""Prepara o S3 efemero usado pelo laboratorio academico.

Execute esta DAG no inicio de cada nova sessao do laboratorio AWS.
Ela nunca remove buckets ou objetos.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from botocore.exceptions import ClientError


LOGGER = logging.getLogger(__name__)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"A variavel de ambiente {name} nao foi configurada.")
    return value


@dag(
    dag_id="aws_lab_s3_bootstrap",
    description="Valida a sessao AWS temporaria e garante o bucket privado do projeto",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "projeto_final_ia",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["aws", "s3", "bootstrap", "laboratorio"],
)
def aws_lab_s3_bootstrap():
    @task
    def ensure_private_bucket() -> dict[str, str | int | bool]:
        import boto3

        bucket_name = required_env("S3_BUCKET_NAME")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip() or "us-east-1"
        prefix = os.getenv("S3_PREFIX", "raw/airflow").strip().strip("/")

        session = boto3.Session(region_name=region)
        credentials = session.get_credentials()
        if credentials is None or not credentials.access_key:
            raise RuntimeError("Nenhuma credencial AWS foi encontrada no container.")
        if not credentials.token:
            raise RuntimeError(
                "AWS_SESSION_TOKEN ausente. O laboratorio exige credenciais temporarias."
            )

        identity = session.client("sts").get_caller_identity()
        s3 = session.client("s3")
        created = False

        try:
            s3.head_bucket(Bucket=bucket_name)
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            error_code = exc.response.get("Error", {}).get("Code", "")
            if status not in {404} and error_code not in {
                "404",
                "NoSuchBucket",
                "NotFound",
            }:
                raise RuntimeError(
                    f"Nao foi possivel acessar o bucket {bucket_name}: {error_code or status}."
                ) from exc

            create_args: dict[str, object] = {"Bucket": bucket_name}
            if region != "us-east-1":
                create_args["CreateBucketConfiguration"] = {
                    "LocationConstraint": region
                }
            s3.create_bucket(**create_args)
            created = True

        try:
            s3.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
        except ClientError as exc:
            LOGGER.warning(
                "O laboratorio nao permitiu reafirmar o bloqueio publico do bucket: %s",
                exc.response.get("Error", {}).get("Code", "erro desconhecido"),
            )

        s3.head_bucket(Bucket=bucket_name)
        sample = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{prefix}/" if prefix else "",
            MaxKeys=1,
        )

        result = {
            "aws_account": identity["Account"],
            "principal_arn": identity["Arn"],
            "bucket": bucket_name,
            "region": region,
            "prefix": prefix,
            "bucket_created": created,
            "prefix_has_objects": bool(sample.get("KeyCount", 0)),
        }
        LOGGER.info("Bootstrap AWS concluido: %s", result)
        return result

    ensure_private_bucket()


aws_lab_s3_bootstrap()
