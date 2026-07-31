"""Busca um lote de mapas a partir dos acidentes no Snowflake e grava no S3.

O manifesto fica no Snowflake. Um marcador criado dentro do proprio bucket
identifica cada nova encarnacao do S3 efemero do laboratorio AWS.
"""

from __future__ import annotations

import hashlib
import logging
import os
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import PurePosixPath

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task


LOGGER = logging.getLogger(__name__)
SNOWFLAKE_CONN_ID = "snowflake_default"
GENERATION_OBJECT_KEY = "_pipeline/storage_generation.txt"


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


def int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw_value = os.getenv(name, str(default)).strip()
    value = int(raw_value)
    if value < minimum or value > maximum:
        raise ValueError(f"{name} deve estar entre {minimum} e {maximum}.")
    return value


def float_env(name: str, default: float, minimum: float, maximum: float) -> float:
    value = float(os.getenv(name, str(default)).strip().replace(",", "."))
    if value < minimum or value > maximum:
        raise ValueError(f"{name} deve estar entre {minimum} e {maximum}.")
    return value


def get_storage_generation(s3_client, bucket: str) -> str:
    """Retorna um identificador que muda quando o bucket efemero e recriado."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=GENERATION_OBJECT_KEY)
        generation = response["Body"].read().decode("ascii").strip()
        uuid.UUID(generation)
        return generation
    except s3_client.exceptions.NoSuchKey:
        generation = str(uuid.uuid4())
        s3_client.put_object(
            Bucket=bucket,
            Key=GENERATION_OBJECT_KEY,
            Body=generation.encode("ascii"),
            ContentType="text/plain",
        )
        return generation


@dag(
    dag_id="snowflake_mapbox_images_to_s3",
    description="Le coordenadas no Snowflake, busca mapas e registra o manifesto no S3/Snowflake",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "projeto_final_ia",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["snowflake", "mapbox", "s3", "ml"],
)
def snowflake_mapbox_images_to_s3():
    @task(execution_timeout=timedelta(hours=1))
    def fetch_image_batch() -> dict[str, int | str]:
        import boto3
        import requests

        bucket = required_env("S3_BUCKET_NAME")
        region = required_env("AWS_DEFAULT_REGION")
        database = sql_identifier(required_env("SNOWFLAKE_DATABASE"))
        mapbox_token = required_env("MAPBOX_ACCESS_TOKEN")
        mapbox_style = os.getenv("MAPBOX_STYLE", "mapbox/streets-v12").strip()
        if "/" not in mapbox_style:
            raise ValueError("MAPBOX_STYLE deve usar o formato usuario/style_id.")

        image_prefix = os.getenv(
            "S3_MAPBOX_IMAGES_PREFIX", "mapbox/static-images"
        ).strip("/")
        batch_size = int_env("ML_IMAGE_BATCH_SIZE", 5, 1, 1000)
        width = int_env("MAPBOX_IMAGE_WIDTH", 400, 1, 1280)
        height = int_env("MAPBOX_IMAGE_HEIGHT", 300, 1, 1280)
        zoom = float_env("MAPBOX_ZOOM", 17.0, 0.0, 22.0)
        timeout = int_env("MAPBOX_REQUEST_TIMEOUT_SECONDS", 30, 1, 300)

        s3_client = boto3.client("s3", region_name=region)
        generation = get_storage_generation(s3_client, bucket)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        connection = hook.get_conn()
        cursor = connection.cursor()
        image_manifest = f"{database}.ML.IMAGE_MANIFEST"
        accidents_table = f"{database}.INTERMEDIATE.INT_ACIDENTES"

        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {image_manifest} (
                    COORDINATE_KEY VARCHAR NOT NULL,
                    LATITUDE NUMBER(12, 8) NOT NULL,
                    LONGITUDE NUMBER(12, 8) NOT NULL,
                    ACCIDENT_COUNT NUMBER NOT NULL,
                    STORAGE_GENERATION VARCHAR NOT NULL,
                    S3_BUCKET VARCHAR NOT NULL,
                    S3_OBJECT_KEY VARCHAR NOT NULL,
                    CONTENT_TYPE VARCHAR,
                    ETAG VARCHAR,
                    MAPBOX_STYLE VARCHAR,
                    MAPBOX_ZOOM FLOAT,
                    IMAGE_WIDTH NUMBER,
                    IMAGE_HEIGHT NUMBER,
                    FETCHED_AT TIMESTAMP_TZ,
                    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )

            cursor.execute(
                f"""
                WITH coordinates AS (
                    SELECT
                        LATITUDE,
                        LONGITUDE,
                        COUNT(*) AS ACCIDENT_COUNT,
                        MIN(CD_BAT) AS FIRST_CD_BAT
                    FROM {accidents_table}
                    WHERE TARGET_COM_VITIMAS IS NOT NULL
                    GROUP BY LATITUDE, LONGITUDE
                )
                SELECT
                    c.LATITUDE,
                    c.LONGITUDE,
                    c.ACCIDENT_COUNT
                FROM coordinates c
                LEFT JOIN {image_manifest} m
                  ON m.LATITUDE = c.LATITUDE
                 AND m.LONGITUDE = c.LONGITUDE
                 AND m.STORAGE_GENERATION = %s
                WHERE m.COORDINATE_KEY IS NULL
                ORDER BY c.FIRST_CD_BAT
                LIMIT %s
                """,
                (generation, batch_size),
            )
            candidates = cursor.fetchall()

            uploaded = 0
            for latitude_raw, longitude_raw, accident_count in candidates:
                latitude = Decimal(latitude_raw)
                longitude = Decimal(longitude_raw)
                latitude_text = f"{latitude:.6f}"
                longitude_text = f"{longitude:.6f}"
                coordinate_key = f"{latitude_text},{longitude_text}"

                style_user, style_id = mapbox_style.split("/", maxsplit=1)
                url = (
                    f"https://api.mapbox.com/styles/v1/{style_user}/{style_id}/static/"
                    f"{longitude_text},{latitude_text},{zoom:.2f},0,0/{width}x{height}"
                )
                response = requests.get(
                    url,
                    params={"access_token": mapbox_token},
                    timeout=(10, timeout),
                )
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "").split(";")[0]
                if content_type != "image/png":
                    raise RuntimeError(
                        f"Mapbox retornou {content_type!r} para "
                        f"{coordinate_key}; esperado image/png."
                    )

                render_key = (
                    f"{coordinate_key}|{mapbox_style}|{zoom:.2f}|{width}x{height}"
                )
                image_hash = hashlib.sha256(render_key.encode("utf-8")).hexdigest()[:24]
                filename = f"map_{image_hash}.png"
                object_key = str(PurePosixPath(image_prefix) / filename)

                put_result = s3_client.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=response.content,
                    ContentType=content_type,
                    Metadata={
                        "latitude": latitude_text,
                        "longitude": longitude_text,
                        "coordinate-key": coordinate_key,
                    },
                )
                etag = put_result.get("ETag", "").strip('"')

                cursor.execute(
                    f"""
                    MERGE INTO {image_manifest} target
                    USING (
                        SELECT
                            %s AS COORDINATE_KEY,
                            %s AS LATITUDE,
                            %s AS LONGITUDE,
                            %s AS ACCIDENT_COUNT,
                            %s AS STORAGE_GENERATION,
                            %s AS S3_BUCKET,
                            %s AS S3_OBJECT_KEY,
                            %s AS CONTENT_TYPE,
                            %s AS ETAG,
                            %s AS MAPBOX_STYLE,
                            %s AS MAPBOX_ZOOM,
                            %s AS IMAGE_WIDTH,
                            %s AS IMAGE_HEIGHT
                    ) source
                    ON target.COORDINATE_KEY = source.COORDINATE_KEY
                   AND target.STORAGE_GENERATION = source.STORAGE_GENERATION
                    WHEN MATCHED THEN UPDATE SET
                        ACCIDENT_COUNT = source.ACCIDENT_COUNT,
                        S3_BUCKET = source.S3_BUCKET,
                        S3_OBJECT_KEY = source.S3_OBJECT_KEY,
                        CONTENT_TYPE = source.CONTENT_TYPE,
                        ETAG = source.ETAG,
                        MAPBOX_STYLE = source.MAPBOX_STYLE,
                        MAPBOX_ZOOM = source.MAPBOX_ZOOM,
                        IMAGE_WIDTH = source.IMAGE_WIDTH,
                        IMAGE_HEIGHT = source.IMAGE_HEIGHT,
                        FETCHED_AT = CURRENT_TIMESTAMP(),
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        COORDINATE_KEY, LATITUDE, LONGITUDE, ACCIDENT_COUNT,
                        STORAGE_GENERATION, S3_BUCKET, S3_OBJECT_KEY, CONTENT_TYPE,
                        ETAG, MAPBOX_STYLE, MAPBOX_ZOOM, IMAGE_WIDTH, IMAGE_HEIGHT,
                        FETCHED_AT, UPDATED_AT
                    ) VALUES (
                        source.COORDINATE_KEY, source.LATITUDE, source.LONGITUDE,
                        source.ACCIDENT_COUNT, source.STORAGE_GENERATION,
                        source.S3_BUCKET, source.S3_OBJECT_KEY, source.CONTENT_TYPE,
                        source.ETAG, source.MAPBOX_STYLE, source.MAPBOX_ZOOM,
                        source.IMAGE_WIDTH, source.IMAGE_HEIGHT,
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    )
                    """,
                    (
                        coordinate_key,
                        latitude,
                        longitude,
                        int(accident_count),
                        generation,
                        bucket,
                        object_key,
                        content_type,
                        etag,
                        mapbox_style,
                        zoom,
                        width,
                        height,
                    ),
                )
                connection.commit()
                uploaded += 1
                LOGGER.info(
                    "Imagem %s/%s enviada para s3://%s/%s",
                    uploaded,
                    len(candidates),
                    bucket,
                    object_key,
                )

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {image_manifest}
                WHERE STORAGE_GENERATION = %s
                """,
                (generation,),
            )
            generation_total = int(cursor.fetchone()[0])
        finally:
            cursor.close()
            connection.close()

        LOGGER.info(
            "Lote Mapbox concluido: %s imagens novas; %s no bucket atual.",
            uploaded,
            generation_total,
        )
        return {
            "uploaded_this_run": uploaded,
            "manifest_rows_current_bucket": generation_total,
            "batch_size": batch_size,
            "bucket": bucket,
        }

    fetch_image_batch()


snowflake_mapbox_images_to_s3()
