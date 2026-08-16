from __future__ import annotations

import hashlib
import os
import shutil
import zipfile
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import pendulum
from airflow.sdk import dag, task

from parquet_utils import convert_csv_to_parquet_file
from pipeline_constants import RAW_COLUMNS


# ============================================================================
# Diretórios compartilhados pelos containers do Airflow
# ============================================================================
DATA_DIRECTORY = Path("/opt/airflow/data")
DOWNLOAD_DIRECTORY = DATA_DIRECTORY / "downloads"
EXTRACTED_DIRECTORY = DATA_DIRECTORY / "extracted"
PARQUET_DIRECTORY = DATA_DIRECTORY / "parquet"


# ============================================================================
# Funções auxiliares
# ============================================================================
def get_required_env(name: str) -> str:
    """
    Recupera uma variável de ambiente obrigatória.

    Raises:
        RuntimeError: quando a variável não está definida ou está vazia.
    """
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"A variável de ambiente obrigatória '{name}' não foi definida."
        )

    return value


def calculate_sha256(file_path: Path) -> str:
    """
    Calcula o hash SHA-256 sem carregar o arquivo inteiro na memória.
    """
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def validate_csv_file(file_path: Path) -> dict[str, Any]:
    """
    Executa validações simples no CSV sem carregá-lo completamente na memória.

    A validação confirma:
    - existência;
    - tamanho maior que zero;
    - ausência de conteúdo HTML;
    - possibilidade de leitura como texto.

    Não tenta interpretar todas as linhas do dataset.
    """
    if not file_path.exists():
        raise FileNotFoundError(
            f"O arquivo CSV não foi encontrado: {file_path}"
        )

    file_size = file_path.stat().st_size

    if file_size <= 0:
        raise RuntimeError(
            f"O arquivo CSV está vazio: {file_path}"
        )

    with file_path.open("rb") as file:
        sample = file.read(64 * 1024)

    lower_sample = sample.lower()

    if b"<html" in lower_sample or b"<!doctype html" in lower_sample:
        raise ValueError(
            "O conteúdo extraído parece ser HTML, e não um arquivo CSV."
        )

    detected_encoding = None

    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            sample.decode(encoding)
            detected_encoding = encoding
            break
        except UnicodeDecodeError:
            continue

    if detected_encoding is None:
        raise ValueError(
            "Não foi possível interpretar o início do arquivo como texto."
        )

    return {
        "detected_encoding": detected_encoding,
        "sample_size_bytes": len(sample),
    }


# ============================================================================
# DAG
# ============================================================================
@dag(
    dag_id="google_drive_zip_csv_to_parquet_s3",
    description=(
        "Baixa um ZIP do Google Drive, extrai o arquivo CSV "
        "e publica uma copia Parquet no Amazon S3."
    ),
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "especializacao",
        "google-drive",
        "zip",
        "csv",
        "parquet",
        "aws",
        "s3",
    ],
)
def google_drive_zip_csv_to_parquet_s3():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def download_zip_from_google_drive() -> dict[str, Any]:
        """
        Baixa o arquivo ZIP público do Google Drive.
        """
        import gdown

        google_drive_file_id = get_required_env(
            "GOOGLE_DRIVE_FILE_ID"
        )

        configured_filename = os.getenv(
            "SOURCE_FILENAME",
            "acidentes2026_todas_causas_tipos.zip",
        ).strip()

        safe_filename = Path(configured_filename).name

        if not safe_filename.lower().endswith(".zip"):
            raise ValueError(
                "SOURCE_FILENAME deve possuir a extensão .zip."
            )

        DOWNLOAD_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        zip_path = DOWNLOAD_DIRECTORY / safe_filename

        if zip_path.exists():
            zip_path.unlink()

        downloaded_file = gdown.download(
            id=google_drive_file_id,
            output=str(zip_path),
            quiet=False,
        )

        if downloaded_file is None:
            raise RuntimeError(
                "O gdown não conseguiu baixar o arquivo do Google Drive."
            )

        if not zip_path.exists():
            raise FileNotFoundError(
                f"O arquivo esperado não foi criado: {zip_path}"
            )

        file_size = zip_path.stat().st_size

        if file_size <= 0:
            raise RuntimeError(
                "O arquivo ZIP baixado está vazio."
            )

        if not zipfile.is_zipfile(zip_path):
            raise ValueError(
                "O arquivo baixado não é um ZIP válido. "
                "Verifique o compartilhamento e o ID do Google Drive."
            )

        zip_sha256 = calculate_sha256(zip_path)

        print("=" * 70)
        print("DOWNLOAD CONCLUÍDO")
        print(f"Arquivo: {zip_path}")
        print(f"Tamanho: {file_size} bytes")
        print(f"SHA-256: {zip_sha256}")
        print("=" * 70)

        return {
            "zip_path": str(zip_path),
            "zip_filename": zip_path.name,
            "zip_size_bytes": file_size,
            "zip_sha256": zip_sha256,
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def extract_csv_from_zip(
        download_information: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Procura recursivamente pelo CSV dentro do ZIP e extrai somente
        o arquivo selecionado.

        O CSV pode estar:
        - na raiz do ZIP;
        - dentro de uma pasta;
        - dentro de vários níveis de pastas.
        """
        zip_path = Path(download_information["zip_path"])

        requested_csv = os.getenv(
            "CSV_FILENAME",
            "",
        ).strip()

        max_csv_size_mb = int(
            os.getenv("MAX_CSV_SIZE_MB", "200")
        )

        max_csv_size_bytes = max_csv_size_mb * 1024 * 1024

        EXTRACTED_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        with zipfile.ZipFile(zip_path, mode="r") as archive:
            file_members = [
                member
                for member in archive.infolist()
                if not member.is_dir()
            ]

            print("=" * 70)
            print("CONTEÚDO DO ARQUIVO ZIP")

            if not file_members:
                print("Nenhum arquivo foi encontrado dentro do ZIP.")

            for member in file_members:
                print(
                    f"- {member.filename} "
                    f"({member.file_size} bytes)"
                )

            print("=" * 70)

            csv_members = [
                member
                for member in file_members
                if PurePosixPath(
                    member.filename
                ).suffix.lower() == ".csv"
            ]

            if not csv_members:
                available_files = [
                    member.filename
                    for member in file_members
                ]

                raise FileNotFoundError(
                    f"Nenhum arquivo CSV foi encontrado dentro de "
                    f"{zip_path.name}. "
                    f"Arquivos encontrados: {available_files}"
                )

            if requested_csv:
                requested_csv_name = Path(requested_csv).name.lower()
                requested_csv_path = requested_csv.replace("\\", "/").lower()

                selected_members = [
                    member
                    for member in csv_members
                    if (
                        member.filename.lower() == requested_csv_path
                        or PurePosixPath(
                            member.filename
                        ).name.lower() == requested_csv_name
                    )
                ]

                if not selected_members:
                    available_csv_files = [
                        member.filename
                        for member in csv_members
                    ]

                    raise FileNotFoundError(
                        f"O CSV configurado em CSV_FILENAME não foi "
                        f"encontrado: {requested_csv}. "
                        f"Arquivos CSV disponíveis: "
                        f"{available_csv_files}"
                    )
            else:
                selected_members = csv_members

            if len(selected_members) > 1:
                available_csv_files = [
                    member.filename
                    for member in selected_members
                ]

                raise RuntimeError(
                    "Foram encontrados vários arquivos CSV no ZIP. "
                    "Informe o arquivo desejado na variável "
                    "CSV_FILENAME. "
                    f"Arquivos encontrados: {available_csv_files}"
                )

            selected_member = selected_members[0]

            if selected_member.file_size <= 0:
                raise RuntimeError(
                    f"O CSV {selected_member.filename} está vazio."
                )

            if selected_member.file_size > max_csv_size_bytes:
                raise RuntimeError(
                    f"O CSV possui {selected_member.file_size} bytes, "
                    f"ultrapassando o limite configurado de "
                    f"{max_csv_size_mb} MB."
                )

            # Usa apenas o nome final do arquivo.
            # Assim, uma pasta interna como:
            # acidentes2026_todas_causas_tipos/arquivo.csv
            # não é recriada no diretório de extração.
            csv_filename = PurePosixPath(
                selected_member.filename
            ).name

            csv_path = EXTRACTED_DIRECTORY / csv_filename

            if csv_path.exists():
                csv_path.unlink()

            # Extrai apenas o CSV selecionado.
            # Não utiliza extractall(), evitando extrair arquivos
            # desnecessários e evitando problemas com caminhos internos.
            with archive.open(selected_member, mode="r") as source:
                with csv_path.open("wb") as destination:
                    shutil.copyfileobj(
                        source,
                        destination,
                        length=1024 * 1024,
                    )

        validation = validate_csv_file(csv_path)
        extracted_size = csv_path.stat().st_size
        csv_sha256 = calculate_sha256(csv_path)

        print("=" * 70)
        print("EXTRAÇÃO CONCLUÍDA")
        print(
            "Caminho interno no ZIP: "
            f"{selected_member.filename}"
        )
        print(f"Arquivo extraído: {csv_path}")
        print(f"Tamanho: {extracted_size} bytes")
        print(
            "Codificação detectada na amostra: "
            f"{validation['detected_encoding']}"
        )
        print(f"SHA-256: {csv_sha256}")
        print("=" * 70)

        return {
            **download_information,
            "csv_path": str(csv_path),
            "csv_filename": csv_path.name,
            "csv_original_path_inside_zip": selected_member.filename,
            "csv_size_bytes": extracted_size,
            "csv_sha256": csv_sha256,
            "csv_detected_encoding": validation[
                "detected_encoding"
            ],
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def convert_csv_to_parquet(
        csv_information: dict[str, Any],
    ) -> dict[str, Any]:
        """Converte o CSV validado para Parquet sem inferir tipos analiticos."""
        csv_path = Path(csv_information["csv_path"])
        configured_filename = os.getenv("PARQUET_FILENAME", "").strip()
        parquet_filename = (
            Path(configured_filename).name
            if configured_filename
            else f"{csv_path.stem}.parquet"
        )

        if not parquet_filename.lower().endswith(".parquet"):
            raise ValueError("PARQUET_FILENAME deve possuir a extensao .parquet.")

        parquet_path = PARQUET_DIRECTORY / parquet_filename
        conversion = convert_csv_to_parquet_file(
            csv_path=csv_path,
            parquet_path=parquet_path,
            encoding=csv_information["csv_detected_encoding"],
            expected_columns=RAW_COLUMNS,
        )
        parquet_sha256 = calculate_sha256(parquet_path)
        reduction_percent = round(
            (1 - conversion["parquet_size_bytes"] / csv_information["csv_size_bytes"])
            * 100,
            2,
        )

        print("=" * 70)
        print("CONVERSAO PARA PARQUET CONCLUIDA")
        print(f"Arquivo: {parquet_path}")
        print(f"Linhas: {conversion['parquet_row_count']}")
        print(f"Colunas: {len(conversion['parquet_columns'])}")
        print(f"Compressao: {conversion['parquet_compression']}")
        print(f"Tamanho CSV: {csv_information['csv_size_bytes']} bytes")
        print(f"Tamanho Parquet: {conversion['parquet_size_bytes']} bytes")
        print(f"Reducao de tamanho: {reduction_percent}%")
        print(f"SHA-256 Parquet: {parquet_sha256}")
        print("=" * 70)

        return {
            **csv_information,
            **conversion,
            "parquet_sha256": parquet_sha256,
            "size_reduction_percent": reduction_percent,
        }

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def upload_parquet_to_s3(
        parquet_information: dict[str, Any],
    ) -> dict[str, Any]:
        """Envia somente o Parquet convertido para o Amazon S3."""
        import boto3

        bucket_name = get_required_env("S3_BUCKET_NAME")
        region_name = get_required_env("AWS_DEFAULT_REGION")

        prefix = os.getenv(
            "S3_PREFIX",
            "",
        ).strip("/")

        parquet_path = Path(parquet_information["parquet_path"])
        parquet_filename = parquet_information["parquet_filename"]

        if not parquet_path.exists():
            raise FileNotFoundError(
                f"O arquivo Parquet nao existe: {parquet_path}"
            )

        object_key = (
            f"{prefix}/{parquet_filename}"
            if prefix
            else parquet_filename
        )

        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )

        s3_client.upload_file(
            Filename=str(parquet_path),
            Bucket=bucket_name,
            Key=object_key,
            ExtraArgs={
                "ContentType": "application/vnd.apache.parquet",
                "Metadata": {
                    "source-csv-sha256": parquet_information["csv_sha256"],
                    "parquet-sha256": parquet_information["parquet_sha256"],
                    "row-count": str(parquet_information["parquet_row_count"]),
                },
            },
        )

        s3_uri = f"s3://{bucket_name}/{object_key}"

        print("=" * 70)
        print("UPLOAD CONCLUÍDO")
        print(f"Origem: {parquet_path}")
        print(f"Destino: {s3_uri}")
        print("=" * 70)

        return {
            **parquet_information,
            "bucket_name": bucket_name,
            "object_key": object_key,
            "s3_uri": s3_uri,
        }

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def validate_s3_upload(
        upload_information: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Confirma que o arquivo enviado aparece no bucket e que o tamanho
        no S3 é igual ao tamanho do arquivo local.
        """
        import boto3

        bucket_name = upload_information["bucket_name"]
        object_key = upload_information["object_key"]
        region_name = get_required_env("AWS_DEFAULT_REGION")

        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )

        response = s3_client.head_object(
            Bucket=bucket_name,
            Key=object_key,
        )

        uploaded_size = int(response["ContentLength"])
        local_size = upload_information["parquet_size_bytes"]
        content_type = response.get("ContentType", "")

        if uploaded_size != local_size:
            raise RuntimeError(
                f"O tamanho do objeto no S3 ({uploaded_size} bytes) "
                f"é diferente do tamanho local ({local_size} bytes)."
            )

        if content_type != "application/vnd.apache.parquet":
            raise RuntimeError(
                "O Content-Type do objeto nao corresponde a Parquet: "
                f"{content_type!r}."
            )

        print("=" * 70)
        print("VALIDAÇÃO DO S3 CONCLUÍDA")
        print(f"Objeto: s3://{bucket_name}/{object_key}")
        print(f"Tamanho local: {local_size} bytes")
        print(f"Tamanho no S3: {uploaded_size} bytes")
        print("=" * 70)

        return {
            **upload_information,
            "validated": True,
            "uploaded_size_bytes": uploaded_size,
            "uploaded_content_type": content_type,
        }

    @task
    def display_result(
        result: dict[str, Any],
    ) -> None:
        """
        Exibe o resumo final nos logs do Airflow.
        """
        print("=" * 70)
        print("PIPELINE FINALIZADO COM SUCESSO")
        print("=" * 70)
        print(f"ZIP baixado: {result['zip_filename']}")
        print(
            "CSV dentro do ZIP: "
            f"{result['csv_original_path_inside_zip']}"
        )
        print(f"CSV extraído: {result['csv_filename']}")
        print(
            "Codificação detectada: "
            f"{result['csv_detected_encoding']}"
        )
        print(f"Parquet gerado: {result['parquet_filename']}")
        print(f"Linhas no Parquet: {result['parquet_row_count']}")
        print(f"Destino: {result['s3_uri']}")
        print(f"Tamanho CSV: {result['csv_size_bytes']} bytes")
        print(f"Tamanho Parquet: {result['parquet_size_bytes']} bytes")
        print(f"Reducao: {result['size_reduction_percent']}%")
        print(f"SHA-256 CSV: {result['csv_sha256']}")
        print(f"SHA-256 Parquet: {result['parquet_sha256']}")
        print(f"Upload validado: {result['validated']}")
        print("=" * 70)

    downloaded_zip = download_zip_from_google_drive()

    extracted_csv = extract_csv_from_zip(
        downloaded_zip
    )

    converted_parquet = convert_csv_to_parquet(
        extracted_csv
    )

    uploaded_parquet = upload_parquet_to_s3(
        converted_parquet
    )

    validated_upload = validate_s3_upload(
        uploaded_parquet
    )

    display_result(
        validated_upload
    )


google_drive_zip_csv_to_parquet_s3()
