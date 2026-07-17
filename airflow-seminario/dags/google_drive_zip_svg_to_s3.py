from __future__ import annotations

import hashlib
import os
import shutil
import zipfile
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import Any
from xml.etree import ElementTree

import pendulum
from airflow.sdk import dag, task


# Diretórios compartilhados entre as tarefas.
DATA_DIRECTORY = Path("/opt/airflow/data")
DOWNLOAD_DIRECTORY = DATA_DIRECTORY / "downloads"
EXTRACTED_DIRECTORY = DATA_DIRECTORY / "extracted"


def calculate_sha256(file_path: Path) -> str:
    """
    Calcula o hash SHA-256 sem carregar o arquivo inteiro na memória.
    """

    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def validate_svg(file_path: Path) -> None:
    """
    Confirma que o arquivo extraído é um XML cujo elemento raiz é SVG.
    """

    try:
        root_tag = None

        for _, element in ElementTree.iterparse(
            file_path,
            events=("start",),
        ):
            root_tag = str(element.tag).lower()
            break

        if root_tag is None or not root_tag.endswith("svg"):
            raise ValueError(
                f"O arquivo {file_path.name} não possui um elemento raiz SVG."
            )

    except ElementTree.ParseError as error:
        raise ValueError(
            f"O arquivo {file_path.name} não é um XML/SVG válido."
        ) from error


@dag(
    dag_id="google_drive_zip_svg_to_s3",
    description=(
        "Baixa um ZIP do Google Drive, extrai o arquivo SVG "
        "e envia o SVG para o Amazon S3"
    ),
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    tags=[
        "especializacao",
        "google-drive",
        "zip",
        "svg",
        "aws",
        "s3",
    ],
)
def google_drive_zip_svg_to_s3():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def download_zip_from_google_drive() -> dict[str, Any]:
        """
        Baixa o ZIP público do Google Drive.
        """

        import gdown

        file_id = os.environ["GOOGLE_DRIVE_FILE_ID"]

        configured_filename = os.getenv(
            "SOURCE_FILENAME",
            "acidentes2026_todas_causas_tipos.zip",
        )

        # Evita que um nome configurado escreva fora do diretório permitido.
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

        # Evita aproveitar acidentalmente um download anterior.
        if zip_path.exists():
            zip_path.unlink()

        downloaded_file = gdown.download(
            id=file_id,
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

        if file_size == 0:
            raise RuntimeError(
                "O arquivo ZIP baixado está vazio."
            )

        if not zipfile.is_zipfile(zip_path):
            raise ValueError(
                "O arquivo baixado não é um ZIP válido. "
                "Verifique se o arquivo do Google Drive está público."
            )

        return {
            "zip_path": str(zip_path),
            "zip_filename": zip_path.name,
            "zip_size_bytes": file_size,
            "zip_sha256": calculate_sha256(zip_path),
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def extract_svg_from_zip(
        download_information: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Localiza e extrai somente o SVG de interesse.
        """

        zip_path = Path(download_information["zip_path"])

        requested_svg = os.getenv(
            "SVG_FILENAME",
            "",
        ).strip()

        max_svg_size_mb = int(
            os.getenv("MAX_SVG_SIZE_MB", "100")
        )

        max_svg_size_bytes = max_svg_size_mb * 1024 * 1024

        EXTRACTED_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        with zipfile.ZipFile(zip_path, mode="r") as archive:
            svg_members = [
                member
                for member in archive.infolist()
                if not member.is_dir()
                and PurePosixPath(member.filename).suffix.lower() == ".svg"
            ]

            if not svg_members:
                raise FileNotFoundError(
                    f"Nenhum arquivo SVG foi encontrado dentro de "
                    f"{zip_path.name}."
                )

            if requested_svg:
                selected_members = [
                    member
                    for member in svg_members
                    if (
                        member.filename == requested_svg
                        or PurePosixPath(member.filename).name
                        == Path(requested_svg).name
                    )
                ]

                if not selected_members:
                    available_files = [
                        member.filename
                        for member in svg_members
                    ]

                    raise FileNotFoundError(
                        f"O SVG configurado em SVG_FILENAME "
                        f"não foi encontrado: {requested_svg}. "
                        f"SVGs disponíveis: {available_files}"
                    )

            else:
                selected_members = svg_members

            if len(selected_members) > 1:
                available_files = [
                    member.filename
                    for member in selected_members
                ]

                raise RuntimeError(
                    "Foram encontrados vários arquivos SVG no ZIP. "
                    "Informe o arquivo desejado na variável SVG_FILENAME. "
                    f"Arquivos encontrados: {available_files}"
                )

            selected_member = selected_members[0]

            if selected_member.file_size <= 0:
                raise RuntimeError(
                    f"O SVG {selected_member.filename} está vazio."
                )

            if selected_member.file_size > max_svg_size_bytes:
                raise RuntimeError(
                    f"O SVG possui {selected_member.file_size} bytes, "
                    f"ultrapassando o limite configurado de "
                    f"{max_svg_size_mb} MB."
                )

            # Usa somente o nome final do arquivo, ignorando pastas
            # internas do ZIP.
            svg_filename = PurePosixPath(
                selected_member.filename
            ).name

            svg_path = EXTRACTED_DIRECTORY / svg_filename

            if svg_path.exists():
                svg_path.unlink()

            # Extrai somente o arquivo escolhido.
            # Não utiliza extractall(), evitando extrair conteúdo
            # desnecessário ou caminhos internos do ZIP.
            with archive.open(selected_member, mode="r") as source:
                with svg_path.open("wb") as destination:
                    shutil.copyfileobj(
                        source,
                        destination,
                        length=1024 * 1024,
                    )

        extracted_size = svg_path.stat().st_size

        if extracted_size == 0:
            raise RuntimeError(
                f"O SVG extraído está vazio: {svg_path}"
            )

        validate_svg(svg_path)

        return {
            **download_information,
            "svg_path": str(svg_path),
            "svg_filename": svg_path.name,
            "svg_original_path_inside_zip": selected_member.filename,
            "svg_size_bytes": extracted_size,
            "svg_sha256": calculate_sha256(svg_path),
        }

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def upload_svg_to_s3(
        svg_information: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Envia somente o SVG extraído para o Amazon S3.
        """

        import boto3

        bucket_name = os.environ["S3_BUCKET_NAME"]
        region_name = os.environ["AWS_DEFAULT_REGION"]

        prefix = os.getenv(
            "S3_PREFIX",
            "",
        ).strip("/")

        svg_path = Path(svg_information["svg_path"])
        svg_filename = svg_information["svg_filename"]

        if prefix:
            object_key = f"{prefix}/{svg_filename}"
        else:
            object_key = svg_filename

        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )

        s3_client.upload_file(
            Filename=str(svg_path),
            Bucket=bucket_name,
            Key=object_key,
            ExtraArgs={
                "ContentType": "image/svg+xml",
            },
        )

        return {
            **svg_information,
            "bucket_name": bucket_name,
            "object_key": object_key,
            "s3_uri": f"s3://{bucket_name}/{object_key}",
        }

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def validate_s3_upload(
        upload_information: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Confirma que a chave enviada aparece no bucket.
        """

        import boto3

        bucket_name = upload_information["bucket_name"]
        object_key = upload_information["object_key"]
        region_name = os.environ["AWS_DEFAULT_REGION"]

        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_key,
            MaxKeys=5,
        )

        uploaded_object = next(
            (
                item
                for item in response.get("Contents", [])
                if item["Key"] == object_key
            ),
            None,
        )

        if uploaded_object is None:
            raise RuntimeError(
                f"O objeto não foi localizado após o upload: "
                f"s3://{bucket_name}/{object_key}"
            )

        uploaded_size = uploaded_object["Size"]
        local_size = upload_information["svg_size_bytes"]

        if uploaded_size != local_size:
            raise RuntimeError(
                f"O tamanho do objeto no S3 ({uploaded_size} bytes) "
                f"é diferente do tamanho local ({local_size} bytes)."
            )

        return {
            **upload_information,
            "validated": True,
            "uploaded_size_bytes": uploaded_size,
        }

    @task
    def display_result(
        result: dict[str, Any],
    ) -> None:
        """
        Exibe o resultado final nos logs.
        """

        print("=" * 70)
        print("PIPELINE FINALIZADO COM SUCESSO")
        print("=" * 70)
        print(f"ZIP baixado: {result['zip_filename']}")
        print(
            "SVG interno: "
            f"{result['svg_original_path_inside_zip']}"
        )
        print(f"SVG enviado: {result['svg_filename']}")
        print(f"Destino: {result['s3_uri']}")
        print(f"Tamanho: {result['svg_size_bytes']} bytes")
        print(f"SHA-256: {result['svg_sha256']}")
        print(f"Upload validado: {result['validated']}")
        print("=" * 70)

    downloaded_zip = download_zip_from_google_drive()

    extracted_svg = extract_svg_from_zip(
        downloaded_zip
    )

    uploaded_svg = upload_svg_to_s3(
        extracted_svg
    )

    validated_upload = validate_s3_upload(
        uploaded_svg
    )

    display_result(
        validated_upload
    )


google_drive_zip_svg_to_s3()