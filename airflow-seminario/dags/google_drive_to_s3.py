from __future__ import annotations

import hashlib
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import dag, task


DATA_DIRECTORY = Path("/opt/airflow/data")


def calculate_sha256(file_path: Path) -> str:
    """Calcula o SHA-256 sem carregar o arquivo inteiro na memória."""
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


@dag(
    dag_id="google_drive_to_s3",
    description="Baixa um arquivo público do Google Drive e envia para o Amazon S3",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    tags=["especializacao", "google-drive", "aws", "s3"],
)
def google_drive_to_s3():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def download_from_google_drive() -> dict[str, Any]:
        """Baixa o arquivo compartilhado no Google Drive."""

        import gdown

        file_id = os.environ["GOOGLE_DRIVE_FILE_ID"]

        configured_filename = os.getenv(
            "SOURCE_FILENAME",
            "acidentes2026_todas_causas_tipos.zip",
        )

        # Impede que o nome configurado escreva fora do diretório esperado.
        safe_filename = Path(configured_filename).name

        DATA_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        destination = DATA_DIRECTORY / safe_filename

        # Remove uma eventual cópia de execução anterior.
        if destination.exists():
            destination.unlink()

        downloaded_file = gdown.download(
            id=file_id,
            output=str(destination),
            quiet=False,
        )

        if downloaded_file is None:
            raise RuntimeError(
                "O gdown não conseguiu baixar o arquivo do Google Drive."
            )

        if not destination.exists():
            raise FileNotFoundError(
                f"O arquivo esperado não foi criado: {destination}"
            )

        file_size = destination.stat().st_size

        if file_size == 0:
            raise RuntimeError(
                "O arquivo baixado está vazio."
            )

        sha256 = calculate_sha256(destination)

        return {
            "local_path": str(destination),
            "filename": destination.name,
            "size_bytes": file_size,
            "sha256": sha256,
        }

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def upload_file_to_s3(
        file_information: dict[str, Any],
    ) -> dict[str, Any]:
        """Envia o arquivo local para o Amazon S3."""

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        bucket_name = os.environ["S3_BUCKET_NAME"]
        prefix = os.getenv("S3_PREFIX", "raw/airflow").strip("/")

        local_path = file_information["local_path"]
        filename = file_information["filename"]

        object_key = f"{prefix}/{filename}"

        # aws_conn_id=None faz o boto3 utilizar as credenciais
        # disponíveis nas variáveis de ambiente.
        s3_hook = S3Hook(aws_conn_id=None)

        s3_hook.load_file(
            filename=local_path,
            key=object_key,
            bucket_name=bucket_name,
            replace=True,
        )

        uploaded = s3_hook.check_for_key(
            key=object_key,
            bucket_name=bucket_name,
        )

        if not uploaded:
            raise RuntimeError(
                f"Não foi possível confirmar o objeto "
                f"s3://{bucket_name}/{object_key}"
            )

        return {
            **file_information,
            "bucket": bucket_name,
            "object_key": object_key,
            "s3_uri": f"s3://{bucket_name}/{object_key}",
        }

    @task
    def display_result(
        upload_information: dict[str, Any],
    ) -> None:
        """Registra no log o resultado da execução."""

        print("Upload concluído com sucesso.")
        print(f"Destino: {upload_information['s3_uri']}")
        print(f"Tamanho: {upload_information['size_bytes']} bytes")
        print(f"SHA-256: {upload_information['sha256']}")

    downloaded_file = download_from_google_drive()
    uploaded_file = upload_file_to_s3(downloaded_file)
    display_result(uploaded_file)


google_drive_to_s3()