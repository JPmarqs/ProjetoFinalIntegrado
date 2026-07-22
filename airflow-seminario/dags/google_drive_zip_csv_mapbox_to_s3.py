from __future__ import annotations

import csv
import hashlib
import os
import shutil
import sqlite3
import time
import unicodedata
import zipfile
from datetime import timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path, PurePosixPath
from typing import Any

import pendulum
from airflow.sdk import dag, task


# ============================================================================
# Diretórios compartilhados pelos containers do Airflow
# ============================================================================
DATA_DIRECTORY = Path("/opt/airflow/data")
DOWNLOAD_DIRECTORY = DATA_DIRECTORY / "downloads"
EXTRACTED_DIRECTORY = DATA_DIRECTORY / "extracted"
STATE_DIRECTORY = DATA_DIRECTORY / "state"
REPORT_DIRECTORY = DATA_DIRECTORY / "reports"

MAPBOX_STATE_DATABASE = STATE_DIRECTORY / "mapbox_coordinates.sqlite3"


# ============================================================================
# Funções auxiliares gerais
# ============================================================================
def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"A variável de ambiente obrigatória '{name}' não foi definida."
        )

    return value


def get_int_env(
    name: str,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw_value = os.getenv(name, str(default)).strip()

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            f"A variável {name} deve ser um número inteiro: {raw_value!r}."
        ) from error

    if minimum is not None and value < minimum:
        raise ValueError(
            f"A variável {name} deve ser maior ou igual a {minimum}."
        )

    if maximum is not None and value > maximum:
        raise ValueError(
            f"A variável {name} deve ser menor ou igual a {maximum}."
        )

    return value


def get_float_env(
    name: str,
    default: float,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    raw_value = os.getenv(name, str(default)).strip()

    try:
        value = float(raw_value.replace(",", "."))
    except ValueError as error:
        raise ValueError(
            f"A variável {name} deve ser numérica: {raw_value!r}."
        ) from error

    if minimum is not None and value < minimum:
        raise ValueError(
            f"A variável {name} deve ser maior ou igual a {minimum}."
        )

    if maximum is not None and value > maximum:
        raise ValueError(
            f"A variável {name} deve ser menor ou igual a {maximum}."
        )

    return value


def calculate_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def utc_now_iso() -> str:
    return pendulum.now("UTC").to_iso8601_string()


# ============================================================================
# Funções auxiliares do CSV
# ============================================================================
def detect_text_encoding(file_path: Path) -> str:
    with file_path.open("rb") as file:
        sample = file.read(64 * 1024)

    lower_sample = sample.lower()

    if b"<html" in lower_sample or b"<!doctype html" in lower_sample:
        raise ValueError(
            "O conteúdo extraído parece ser HTML, e não um arquivo CSV."
        )

    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            sample.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue

    raise ValueError(
        "Não foi possível interpretar o início do arquivo como texto."
    )


def resolve_csv_delimiter(file_path: Path, encoding: str) -> str:
    configured_delimiter = os.getenv("CSV_DELIMITER", "").strip()

    if configured_delimiter:
        if configured_delimiter == r"\t":
            return "\t"

        if len(configured_delimiter) != 1:
            raise ValueError(
                "CSV_DELIMITER deve possuir um único caractere, "
                "por exemplo ';', ',' ou '\\t'."
            )

        return configured_delimiter

    with file_path.open(
        "r",
        encoding=encoding,
        newline="",
        errors="replace",
    ) as file:
        sample = file.read(64 * 1024)

    try:
        dialect = csv.Sniffer().sniff(
            sample,
            delimiters=";,\t|",
        )
        return dialect.delimiter
    except csv.Error:
        # Arquivos brasileiros com vírgula decimal normalmente usam
        # ponto e vírgula como separador de colunas.
        return ";"


def normalize_header(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    without_accents = "".join(
        character
        for character in normalized
        if not unicodedata.combining(character)
    )

    return "".join(
        character
        for character in without_accents.lower().strip()
        if character.isalnum()
    )


def resolve_column_name(
    fieldnames: list[str],
    configured_name: str,
    candidates: tuple[str, ...],
    logical_name: str,
) -> str:
    normalized_fields = {
        normalize_header(field): field
        for field in fieldnames
        if field is not None
    }

    names_to_try = []

    if configured_name.strip():
        names_to_try.append(configured_name)

    names_to_try.extend(candidates)

    for name in names_to_try:
        resolved = normalized_fields.get(normalize_header(name))

        if resolved is not None:
            return resolved

    raise KeyError(
        f"Não foi possível localizar a coluna de {logical_name}. "
        f"Colunas disponíveis: {fieldnames}. "
        f"Configure explicitamente a variável correspondente no .env."
    )


def normalize_decimal_text(raw_value: Any) -> str:
    text = str(raw_value).strip()
    text = text.replace("\u00a0", "").replace(" ", "")

    if not text:
        raise ValueError("valor vazio")

    # Somente vírgula: considera vírgula decimal.
    if "," in text and "." not in text:
        text = text.replace(",", ".")

    # Vírgula e ponto: considera como decimal o separador mais à direita.
    elif "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")

    return text


def normalize_coordinate(
    raw_value: Any,
    coordinate_name: str,
    minimum: Decimal,
    maximum: Decimal,
    decimal_places: int,
) -> str:
    try:
        decimal_value = Decimal(
            normalize_decimal_text(raw_value)
        )
    except (InvalidOperation, ValueError) as error:
        raise ValueError(
            f"{coordinate_name} inválida: {raw_value!r}"
        ) from error

    if not decimal_value.is_finite():
        raise ValueError(
            f"{coordinate_name} não é finita: {raw_value!r}"
        )

    if decimal_value < minimum or decimal_value > maximum:
        raise ValueError(
            f"{coordinate_name} fora do intervalo permitido: "
            f"{decimal_value}"
        )

    quantizer = Decimal("1").scaleb(-decimal_places)

    normalized = decimal_value.quantize(
        quantizer,
        rounding=ROUND_HALF_UP,
    )

    # Evita o texto "-0.000000".
    if normalized == 0:
        normalized = abs(normalized)

    return f"{normalized:.{decimal_places}f}"


# ============================================================================
# Funções auxiliares do controle persistente
# ============================================================================
def initialize_state_database(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS mapbox_requests (
            request_key TEXT PRIMARY KEY,
            coordinate_key TEXT NOT NULL,
            latitude TEXT NOT NULL,
            longitude TEXT NOT NULL,
            render_signature TEXT NOT NULL,
            occurrences INTEGER NOT NULL DEFAULT 0,
            present_in_current_csv INTEGER NOT NULL DEFAULT 0,
            first_source_row INTEGER,
            status TEXT NOT NULL DEFAULT 'PENDING',
            attempts INTEGER NOT NULL DEFAULT 0,
            object_key TEXT,
            content_type TEXT,
            etag TEXT,
            http_status INTEGER,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mapbox_requests_processing
        ON mapbox_requests (
            render_signature,
            present_in_current_csv,
            status,
            attempts
        )
        """
    )

    connection.commit()


def build_render_signature() -> str:
    style = os.getenv(
        "MAPBOX_STYLE",
        "mapbox/streets-v12",
    ).strip()

    zoom = get_float_env(
        "MAPBOX_ZOOM",
        17.0,
        minimum=0,
        maximum=22,
    )

    width = get_int_env(
        "MAPBOX_IMAGE_WIDTH",
        400,
        minimum=1,
        maximum=1280,
    )

    height = get_int_env(
        "MAPBOX_IMAGE_HEIGHT",
        300,
        minimum=1,
        maximum=1280,
    )

    return (
        f"style={style}|zoom={zoom:.2f}|"
        f"width={width}|height={height}|bearing=0|pitch=0"
    )


def build_request_key(
    coordinate_key: str,
    render_signature: str,
) -> str:
    content = f"{coordinate_key}|{render_signature}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def safe_coordinate_for_filename(value: str) -> str:
    return value.replace("-", "m").replace(".", "p")


# ============================================================================
# DAG
# ============================================================================
@dag(
    dag_id="google_drive_zip_csv_mapbox_to_s3",
    description=(
        "Baixa um ZIP do Google Drive, extrai e envia o CSV para o S3, "
        "deduplica coordenadas e gera imagens estáticas do Mapbox no S3."
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
        "csv",
        "mapbox",
        "aws",
        "s3",
    ],
)
def google_drive_zip_csv_mapbox_to_s3():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def download_zip_from_google_drive() -> dict[str, Any]:
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
        zip_path = Path(download_information["zip_path"])

        requested_csv = os.getenv(
            "CSV_FILENAME",
            "",
        ).strip()

        max_csv_size_mb = get_int_env(
            "MAX_CSV_SIZE_MB",
            500,
            minimum=1,
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
                requested_csv_name = Path(
                    requested_csv
                ).name.lower()

                requested_csv_path = requested_csv.replace(
                    "\\",
                    "/",
                ).lower()

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
                    "Informe o arquivo desejado em CSV_FILENAME. "
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
                    f"ultrapassando o limite de {max_csv_size_mb} MB."
                )

            csv_filename = PurePosixPath(
                selected_member.filename
            ).name

            csv_path = EXTRACTED_DIRECTORY / csv_filename

            if csv_path.exists():
                csv_path.unlink()

            with archive.open(selected_member, mode="r") as source:
                with csv_path.open("wb") as destination:
                    shutil.copyfileobj(
                        source,
                        destination,
                        length=1024 * 1024,
                    )

        encoding = detect_text_encoding(csv_path)
        delimiter = resolve_csv_delimiter(
            csv_path,
            encoding,
        )

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
        print(f"Codificação: {encoding}")
        print(f"Delimitador: {delimiter!r}")
        print(f"SHA-256: {csv_sha256}")
        print("=" * 70)

        return {
            **download_information,
            "csv_path": str(csv_path),
            "csv_filename": csv_path.name,
            "csv_original_path_inside_zip": selected_member.filename,
            "csv_size_bytes": extracted_size,
            "csv_sha256": csv_sha256,
            "csv_encoding": encoding,
            "csv_delimiter": delimiter,
        }

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def upload_csv_to_s3(
        csv_information: dict[str, Any],
    ) -> dict[str, Any]:
        import boto3

        bucket_name = get_required_env("S3_BUCKET_NAME")
        region_name = get_required_env("AWS_DEFAULT_REGION")

        prefix = os.getenv(
            "S3_CSV_PREFIX",
            os.getenv("S3_PREFIX", "raw/airflow"),
        ).strip("/")

        csv_path = Path(csv_information["csv_path"])
        csv_filename = csv_information["csv_filename"]

        object_key = (
            f"{prefix}/{csv_filename}"
            if prefix
            else csv_filename
        )

        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )

        s3_client.upload_file(
            Filename=str(csv_path),
            Bucket=bucket_name,
            Key=object_key,
            ExtraArgs={
                "ContentType": "text/csv",
            },
        )

        response = s3_client.head_object(
            Bucket=bucket_name,
            Key=object_key,
        )

        uploaded_size = response["ContentLength"]

        if uploaded_size != csv_information["csv_size_bytes"]:
            raise RuntimeError(
                f"O CSV no S3 possui {uploaded_size} bytes, mas o "
                f"arquivo local possui "
                f"{csv_information['csv_size_bytes']} bytes."
            )

        s3_uri = f"s3://{bucket_name}/{object_key}"

        print("=" * 70)
        print("CSV ENVIADO E VALIDADO NO S3")
        print(f"Destino: {s3_uri}")
        print(f"Tamanho: {uploaded_size} bytes")
        print("=" * 70)

        return {
            **csv_information,
            "csv_bucket_name": bucket_name,
            "csv_object_key": object_key,
            "csv_s3_uri": s3_uri,
            "csv_upload_validated": True,
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def prepare_coordinate_queue(
        csv_information: dict[str, Any],
    ) -> dict[str, Any]:
        csv_path = Path(csv_information["csv_path"])
        encoding = csv_information["csv_encoding"]
        delimiter = csv_information["csv_delimiter"]

        latitude_configured = os.getenv(
            "CSV_LATITUDE_COLUMN",
            "latitude",
        )

        longitude_configured = os.getenv(
            "CSV_LONGITUDE_COLUMN",
            "longitude",
        )

        decimal_places = get_int_env(
            "COORDINATE_DECIMAL_PLACES",
            6,
            minimum=0,
            maximum=10,
        )

        render_signature = build_render_signature()

        STATE_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        total_rows = 0
        valid_rows = 0
        invalid_rows = 0
        invalid_examples: list[str] = []

        with sqlite3.connect(
            MAPBOX_STATE_DATABASE,
            timeout=60,
        ) as connection:
            initialize_state_database(connection)

            connection.execute(
                """
                UPDATE mapbox_requests
                SET occurrences = 0,
                    present_in_current_csv = 0,
                    updated_at = ?
                WHERE render_signature = ?
                """,
                (
                    utc_now_iso(),
                    render_signature,
                ),
            )

            with csv_path.open(
                "r",
                encoding=encoding,
                newline="",
                errors="replace",
            ) as csv_file:
                reader = csv.DictReader(
                    csv_file,
                    delimiter=delimiter,
                )

                if not reader.fieldnames:
                    raise RuntimeError(
                        "O arquivo CSV não possui cabeçalho."
                    )

                latitude_column = resolve_column_name(
                    reader.fieldnames,
                    latitude_configured,
                    (
                        "latitude",
                        "lat",
                        "latitude_acidente",
                        "latitude acidente",
                    ),
                    "latitude",
                )

                longitude_column = resolve_column_name(
                    reader.fieldnames,
                    longitude_configured,
                    (
                        "longitude",
                        "lon",
                        "lng",
                        "longitude_acidente",
                        "longitude acidente",
                    ),
                    "longitude",
                )

                print("=" * 70)
                print("PREPARAÇÃO DAS COORDENADAS")
                print(f"Coluna de latitude: {latitude_column}")
                print(f"Coluna de longitude: {longitude_column}")
                print(f"Casas decimais: {decimal_places}")
                print(f"Renderização: {render_signature}")
                print("=" * 70)

                for source_row, row in enumerate(
                    reader,
                    start=2,
                ):
                    total_rows += 1

                    try:
                        latitude = normalize_coordinate(
                            row.get(latitude_column),
                            "latitude",
                            Decimal("-85.0511"),
                            Decimal("85.0511"),
                            decimal_places,
                        )

                        longitude = normalize_coordinate(
                            row.get(longitude_column),
                            "longitude",
                            Decimal("-180"),
                            Decimal("180"),
                            decimal_places,
                        )

                    except ValueError as error:
                        invalid_rows += 1

                        if len(invalid_examples) < 20:
                            invalid_examples.append(
                                f"Linha {source_row}: {error}"
                            )

                        continue

                    valid_rows += 1

                    coordinate_key = (
                        f"{latitude},{longitude}"
                    )

                    request_key = build_request_key(
                        coordinate_key,
                        render_signature,
                    )

                    now = utc_now_iso()

                    connection.execute(
                        """
                        INSERT INTO mapbox_requests (
                            request_key,
                            coordinate_key,
                            latitude,
                            longitude,
                            render_signature,
                            occurrences,
                            present_in_current_csv,
                            first_source_row,
                            status,
                            attempts,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, 1, 1, ?, 'PENDING', 0, ?, ?)
                        ON CONFLICT(request_key) DO UPDATE SET
                            occurrences = mapbox_requests.occurrences + 1,
                            present_in_current_csv = 1,
                            updated_at = excluded.updated_at
                        """,
                        (
                            request_key,
                            coordinate_key,
                            latitude,
                            longitude,
                            render_signature,
                            source_row,
                            now,
                            now,
                        ),
                    )

                    if total_rows % 5000 == 0:
                        connection.commit()
                        print(
                            f"Linhas processadas: {total_rows}"
                        )

            connection.commit()

            unique_coordinates = connection.execute(
                """
                SELECT COUNT(*)
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                """,
                (render_signature,),
            ).fetchone()[0]

            already_uploaded = connection.execute(
                """
                SELECT COUNT(*)
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                  AND status = 'UPLOADED'
                """,
                (render_signature,),
            ).fetchone()[0]

            pending_coordinates = connection.execute(
                """
                SELECT COUNT(*)
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                  AND status <> 'UPLOADED'
                """,
                (render_signature,),
            ).fetchone()[0]

        duplicate_rows = valid_rows - unique_coordinates

        print("=" * 70)
        print("FILA DE COORDENADAS PREPARADA")
        print(f"Linhas totais: {total_rows}")
        print(f"Linhas com coordenadas válidas: {valid_rows}")
        print(f"Linhas inválidas: {invalid_rows}")
        print(f"Coordenadas únicas: {unique_coordinates}")
        print(f"Duplicidades eliminadas: {duplicate_rows}")
        print(f"Já enviadas anteriormente: {already_uploaded}")
        print(f"Pendentes: {pending_coordinates}")

        if invalid_examples:
            print("Exemplos de linhas ignoradas:")

            for example in invalid_examples:
                print(f"- {example}")

        print("=" * 70)

        return {
            **csv_information,
            "state_database_path": str(
                MAPBOX_STATE_DATABASE
            ),
            "render_signature": render_signature,
            "coordinate_decimal_places": decimal_places,
            "coordinate_total_rows": total_rows,
            "coordinate_valid_rows": valid_rows,
            "coordinate_invalid_rows": invalid_rows,
            "coordinate_unique_count": unique_coordinates,
            "coordinate_duplicate_rows": duplicate_rows,
            "coordinate_already_uploaded": already_uploaded,
            "coordinate_pending_count": pending_coordinates,
        }

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=12),
    )
    def fetch_mapbox_images_to_s3(
        queue_information: dict[str, Any],
        csv_upload_information: dict[str, Any],
    ) -> dict[str, Any]:
        import boto3
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        # Este parâmetro é recebido para criar dependência explícita:
        # as imagens só começam após a validação do CSV no S3.
        del csv_upload_information

        mapbox_token = get_required_env(
            "MAPBOX_ACCESS_TOKEN"
        )

        mapbox_style = os.getenv(
            "MAPBOX_STYLE",
            "mapbox/streets-v12",
        ).strip()

        if "/" not in mapbox_style:
            raise ValueError(
                "MAPBOX_STYLE deve seguir o padrão "
                "'usuario/style_id', por exemplo "
                "'mapbox/streets-v12'."
            )

        style_username, style_id = mapbox_style.split(
            "/",
            maxsplit=1,
        )

        zoom = get_float_env(
            "MAPBOX_ZOOM",
            17.0,
            minimum=0,
            maximum=22,
        )

        image_width = get_int_env(
            "MAPBOX_IMAGE_WIDTH",
            400,
            minimum=1,
            maximum=1280,
        )

        image_height = get_int_env(
            "MAPBOX_IMAGE_HEIGHT",
            300,
            minimum=1,
            maximum=1280,
        )

        requests_per_minute = get_int_env(
            "MAPBOX_REQUESTS_PER_MINUTE",
            300,
            minimum=1,
            maximum=1250,
        )

        max_images_per_run = get_int_env(
            "MAPBOX_MAX_IMAGES_PER_RUN",
            500,
            minimum=0,
        )

        max_attempts = get_int_env(
            "MAPBOX_MAX_ATTEMPTS_PER_COORDINATE",
            3,
            minimum=1,
            maximum=20,
        )

        request_timeout_seconds = get_int_env(
            "MAPBOX_REQUEST_TIMEOUT_SECONDS",
            30,
            minimum=1,
            maximum=300,
        )

        bucket_name = get_required_env("S3_BUCKET_NAME")
        region_name = get_required_env("AWS_DEFAULT_REGION")

        image_prefix = os.getenv(
            "S3_MAPBOX_IMAGES_PREFIX",
            "mapbox/static-images",
        ).strip("/")

        state_database_path = Path(
            queue_information["state_database_path"]
        )

        render_signature = queue_information[
            "render_signature"
        ]

        delay_between_requests = (
            60.0 / requests_per_minute
        )

        retry_configuration = Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=1,
            status_forcelist=(
                429,
                500,
                502,
                503,
                504,
            ),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )

        session = requests.Session()
        session.mount(
            "https://",
            HTTPAdapter(
                max_retries=retry_configuration
            ),
        )

        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )

        processed = 0
        uploaded = 0
        permanent_failures = 0
        last_request_started_at: float | None = None

        with sqlite3.connect(
            state_database_path,
            timeout=60,
        ) as connection:
            initialize_state_database(connection)

            # Recupera registros que ficaram como PROCESSING caso uma
            # execução anterior tenha sido interrompida.
            connection.execute(
                """
                UPDATE mapbox_requests
                SET status = 'PENDING',
                    updated_at = ?
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                  AND status = 'PROCESSING'
                """,
                (
                    utc_now_iso(),
                    render_signature,
                ),
            )

            connection.commit()

            query = """
                SELECT
                    request_key,
                    coordinate_key,
                    latitude,
                    longitude,
                    attempts
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                  AND status <> 'UPLOADED'
                  AND attempts < ?
                ORDER BY first_source_row, coordinate_key
            """

            parameters: list[Any] = [
                render_signature,
                max_attempts,
            ]

            if max_images_per_run > 0:
                query += " LIMIT ?"
                parameters.append(max_images_per_run)

            pending_rows = connection.execute(
                query,
                parameters,
            ).fetchall()

            print("=" * 70)
            print("CONSULTA À API MAPBOX")
            print(f"Estilo: {mapbox_style}")
            print(f"Zoom: {zoom}")
            print(
                f"Dimensão: {image_width}x{image_height}"
            )
            print(
                "Limite configurado: "
                f"{requests_per_minute} requisições/minuto"
            )
            print(
                "Coordenadas selecionadas nesta execução: "
                f"{len(pending_rows)}"
            )
            print(f"Destino S3: s3://{bucket_name}/{image_prefix}")
            print("=" * 70)

            for (
                request_key,
                coordinate_key,
                latitude,
                longitude,
                attempts,
            ) in pending_rows:
                now = utc_now_iso()

                connection.execute(
                    """
                    UPDATE mapbox_requests
                    SET status = 'PROCESSING',
                        attempts = attempts + 1,
                        error = NULL,
                        updated_at = ?
                    WHERE request_key = ?
                    """,
                    (
                        now,
                        request_key,
                    ),
                )

                connection.commit()

                if last_request_started_at is not None:
                    elapsed = (
                        time.monotonic()
                        - last_request_started_at
                    )

                    remaining_delay = (
                        delay_between_requests - elapsed
                    )

                    if remaining_delay > 0:
                        time.sleep(remaining_delay)

                last_request_started_at = time.monotonic()

                url = (
                    "https://api.mapbox.com/styles/v1/"
                    f"{style_username}/{style_id}/static/"
                    f"{longitude},{latitude},{zoom:.2f},0,0/"
                    f"{image_width}x{image_height}"
                )

                try:
                    response = session.get(
                        url,
                        params={
                            "access_token": mapbox_token,
                        },
                        timeout=(
                            10,
                            request_timeout_seconds,
                        ),
                    )

                    http_status = response.status_code

                    if http_status in (401, 403):
                        error_message = (
                            f"Falha de autenticação/autorização "
                            f"Mapbox: HTTP {http_status}. "
                            f"Resposta: {response.text[:500]}"
                        )

                        connection.execute(
                            """
                            UPDATE mapbox_requests
                            SET status = 'FAILED',
                                http_status = ?,
                                error = ?,
                                updated_at = ?
                            WHERE request_key = ?
                            """,
                            (
                                http_status,
                                error_message,
                                utc_now_iso(),
                                request_key,
                            ),
                        )

                        connection.commit()
                        raise RuntimeError(error_message)

                    if http_status == 429 or http_status >= 500:
                        error_message = (
                            f"Falha temporária da API Mapbox: "
                            f"HTTP {http_status}. "
                            f"Resposta: {response.text[:500]}"
                        )

                        connection.execute(
                            """
                            UPDATE mapbox_requests
                            SET status = 'FAILED',
                                http_status = ?,
                                error = ?,
                                updated_at = ?
                            WHERE request_key = ?
                            """,
                            (
                                http_status,
                                error_message,
                                utc_now_iso(),
                                request_key,
                            ),
                        )

                        connection.commit()
                        raise RuntimeError(error_message)

                    if not response.ok:
                        error_message = (
                            f"Mapbox retornou HTTP {http_status}. "
                            f"Resposta: {response.text[:500]}"
                        )

                        connection.execute(
                            """
                            UPDATE mapbox_requests
                            SET status = 'FAILED',
                                http_status = ?,
                                error = ?,
                                updated_at = ?
                            WHERE request_key = ?
                            """,
                            (
                                http_status,
                                error_message,
                                utc_now_iso(),
                                request_key,
                            ),
                        )

                        connection.commit()
                        permanent_failures += 1
                        processed += 1
                        print(
                            f"[FALHA] {coordinate_key}: "
                            f"HTTP {http_status}"
                        )
                        continue

                    content_type = response.headers.get(
                        "Content-Type",
                        "",
                    ).split(";")[0].strip().lower()

                    if content_type == "image/png":
                        extension = "png"
                    elif content_type in (
                        "image/jpeg",
                        "image/jpg",
                    ):
                        extension = "jpg"
                    else:
                        error_message = (
                            "A API Mapbox não retornou uma imagem. "
                            f"Content-Type: {content_type!r}. "
                            f"Resposta: {response.text[:500]}"
                        )

                        connection.execute(
                            """
                            UPDATE mapbox_requests
                            SET status = 'FAILED',
                                http_status = ?,
                                error = ?,
                                updated_at = ?
                            WHERE request_key = ?
                            """,
                            (
                                http_status,
                                error_message,
                                utc_now_iso(),
                                request_key,
                            ),
                        )

                        connection.commit()
                        permanent_failures += 1
                        processed += 1
                        print(
                            f"[FALHA] {coordinate_key}: "
                            "resposta não é imagem"
                        )
                        continue

                    safe_latitude = safe_coordinate_for_filename(
                        latitude
                    )

                    safe_longitude = safe_coordinate_for_filename(
                        longitude
                    )

                    render_hash = hashlib.sha256(
                        render_signature.encode("utf-8")
                    ).hexdigest()[:10]

                    filename = (
                        f"lat_{safe_latitude}_"
                        f"lon_{safe_longitude}_"
                        f"{render_hash}.{extension}"
                    )

                    object_key = (
                        f"{image_prefix}/{filename}"
                        if image_prefix
                        else filename
                    )

                    put_response = s3_client.put_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        Body=response.content,
                        ContentType=content_type,
                        Metadata={
                            "latitude": latitude,
                            "longitude": longitude,
                            "mapbox-style": mapbox_style,
                            "mapbox-zoom": f"{zoom:.2f}",
                            "coordinate-key": coordinate_key,
                        },
                    )

                    etag = put_response.get(
                        "ETag",
                        "",
                    ).strip('"')

                    connection.execute(
                        """
                        UPDATE mapbox_requests
                        SET status = 'UPLOADED',
                            object_key = ?,
                            content_type = ?,
                            etag = ?,
                            http_status = ?,
                            error = NULL,
                            updated_at = ?
                        WHERE request_key = ?
                        """,
                        (
                            object_key,
                            content_type,
                            etag,
                            http_status,
                            utc_now_iso(),
                            request_key,
                        ),
                    )

                    connection.commit()

                    uploaded += 1
                    processed += 1

                    print(
                        f"[OK {uploaded}/{len(pending_rows)}] "
                        f"{coordinate_key} -> "
                        f"s3://{bucket_name}/{object_key}"
                    )

                except requests.RequestException as error:
                    error_message = (
                        f"Erro de comunicação com o Mapbox: "
                        f"{type(error).__name__}: {error}"
                    )

                    connection.execute(
                        """
                        UPDATE mapbox_requests
                        SET status = 'FAILED',
                            error = ?,
                            updated_at = ?
                        WHERE request_key = ?
                        """,
                        (
                            error_message,
                            utc_now_iso(),
                            request_key,
                        ),
                    )

                    connection.commit()
                    raise RuntimeError(error_message) from error

            remaining = connection.execute(
                """
                SELECT COUNT(*)
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                  AND status <> 'UPLOADED'
                  AND attempts < ?
                """,
                (
                    render_signature,
                    max_attempts,
                ),
            ).fetchone()[0]

            total_uploaded = connection.execute(
                """
                SELECT COUNT(*)
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                  AND status = 'UPLOADED'
                """,
                (render_signature,),
            ).fetchone()[0]

            exhausted_failures = connection.execute(
                """
                SELECT COUNT(*)
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                  AND status <> 'UPLOADED'
                  AND attempts >= ?
                """,
                (
                    render_signature,
                    max_attempts,
                ),
            ).fetchone()[0]

        print("=" * 70)
        print("LOTE MAPBOX FINALIZADO")
        print(f"Processadas nesta execução: {processed}")
        print(f"Enviadas nesta execução: {uploaded}")
        print(
            "Falhas permanentes nesta execução: "
            f"{permanent_failures}"
        )
        print(f"Total já enviado: {total_uploaded}")
        print(f"Restantes aptas a tentar: {remaining}")
        print(
            "Falhas que atingiram o máximo de tentativas: "
            f"{exhausted_failures}"
        )
        print("=" * 70)

        return {
            **queue_information,
            "mapbox_processed_this_run": processed,
            "mapbox_uploaded_this_run": uploaded,
            "mapbox_permanent_failures_this_run": (
                permanent_failures
            ),
            "mapbox_total_uploaded": total_uploaded,
            "mapbox_remaining": remaining,
            "mapbox_exhausted_failures": exhausted_failures,
            "mapbox_bucket_name": bucket_name,
            "mapbox_images_prefix": image_prefix,
            "mapbox_requests_per_minute": (
                requests_per_minute
            ),
            "mapbox_max_images_per_run": (
                max_images_per_run
            ),
        }

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def export_processing_report_to_s3(
        processing_information: dict[str, Any],
    ) -> dict[str, Any]:
        import boto3

        state_database_path = Path(
            processing_information[
                "state_database_path"
            ]
        )

        render_signature = processing_information[
            "render_signature"
        ]

        bucket_name = processing_information[
            "mapbox_bucket_name"
        ]

        region_name = get_required_env(
            "AWS_DEFAULT_REGION"
        )

        report_prefix = os.getenv(
            "S3_MAPBOX_REPORTS_PREFIX",
            "mapbox/reports",
        ).strip("/")

        REPORT_DIRECTORY.mkdir(
            parents=True,
            exist_ok=True,
        )

        report_path = (
            REPORT_DIRECTORY
            / "mapbox_coordinate_processing_report.csv"
        )

        with sqlite3.connect(
            state_database_path,
            timeout=60,
        ) as connection:
            rows = connection.execute(
                """
                SELECT
                    coordinate_key,
                    latitude,
                    longitude,
                    occurrences,
                    status,
                    attempts,
                    object_key,
                    content_type,
                    http_status,
                    error,
                    updated_at
                FROM mapbox_requests
                WHERE render_signature = ?
                  AND present_in_current_csv = 1
                ORDER BY first_source_row, coordinate_key
                """,
                (render_signature,),
            )

            with report_path.open(
                "w",
                encoding="utf-8",
                newline="",
            ) as report_file:
                writer = csv.writer(
                    report_file,
                    delimiter=";",
                )

                writer.writerow(
                    [
                        "coordinate_key",
                        "latitude",
                        "longitude",
                        "occurrences_no_csv",
                        "status",
                        "attempts",
                        "s3_object_key",
                        "content_type",
                        "http_status",
                        "error",
                        "updated_at",
                    ]
                )

                writer.writerows(rows)

        report_object_key = (
            f"{report_prefix}/{report_path.name}"
            if report_prefix
            else report_path.name
        )

        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )

        s3_client.upload_file(
            Filename=str(report_path),
            Bucket=bucket_name,
            Key=report_object_key,
            ExtraArgs={
                "ContentType": "text/csv",
            },
        )

        report_s3_uri = (
            f"s3://{bucket_name}/{report_object_key}"
        )

        print("=" * 70)
        print("RELATÓRIO DE PROCESSAMENTO ENVIADO")
        print(f"Arquivo local: {report_path}")
        print(f"Destino: {report_s3_uri}")
        print("=" * 70)

        return {
            **processing_information,
            "mapbox_report_path": str(report_path),
            "mapbox_report_object_key": (
                report_object_key
            ),
            "mapbox_report_s3_uri": report_s3_uri,
        }

    @task
    def display_result(
        result: dict[str, Any],
    ) -> None:
        print("=" * 70)
        print("PIPELINE FINALIZADO")
        print("=" * 70)
        print(f"ZIP baixado: {result['zip_filename']}")
        print(f"CSV extraído: {result['csv_filename']}")
        print(
            f"Coordenadas válidas: "
            f"{result['coordinate_valid_rows']}"
        )
        print(
            f"Coordenadas únicas: "
            f"{result['coordinate_unique_count']}"
        )
        print(
            f"Duplicidades eliminadas: "
            f"{result['coordinate_duplicate_rows']}"
        )
        print(
            f"Imagens enviadas nesta execução: "
            f"{result['mapbox_uploaded_this_run']}"
        )
        print(
            f"Total de imagens enviadas: "
            f"{result['mapbox_total_uploaded']}"
        )
        print(
            f"Coordenadas ainda pendentes: "
            f"{result['mapbox_remaining']}"
        )
        print(
            f"Falhas sem novas tentativas: "
            f"{result['mapbox_exhausted_failures']}"
        )
        print(
            f"Relatório: "
            f"{result['mapbox_report_s3_uri']}"
        )
        print("=" * 70)

        if result["mapbox_remaining"] > 0:
            print(
                "Ainda existem coordenadas pendentes. "
                "Execute a DAG novamente para processar o "
                "próximo lote ou aumente "
                "MAPBOX_MAX_IMAGES_PER_RUN."
            )

    downloaded_zip = download_zip_from_google_drive()

    extracted_csv = extract_csv_from_zip(
        downloaded_zip
    )

    uploaded_csv = upload_csv_to_s3(
        extracted_csv
    )

    coordinate_queue = prepare_coordinate_queue(
        extracted_csv
    )

    processed_images = fetch_mapbox_images_to_s3(
        coordinate_queue,
        uploaded_csv,
    )

    processing_report = export_processing_report_to_s3(
        processed_images
    )

    display_result(
        processing_report
    )


google_drive_zip_csv_mapbox_to_s3()