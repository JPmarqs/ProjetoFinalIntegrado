"""Pipeline integrado Snowflake + imagens S3 + Random Forest.

O modulo e chamado por uma DAG, mas concentra a logica de ML fora do arquivo
de orquestracao para permitir testes e evolucao independentes.
"""

from __future__ import annotations

from io import BytesIO
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
import uuid

import joblib
import numpy as np
import pandas as pd
from PIL import Image
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


LOGGER = logging.getLogger(__name__)
SNOWFLAKE_CONN_ID = "snowflake_default"
GENERATION_OBJECT_KEY = "_pipeline/storage_generation.txt"
MODEL_NAME = "random_forest_vitimas_v1"

CATEGORICAL_FEATURES = [
    "UF",
    "DIA_SEMANA",
    "CAUSA_ACIDENTE",
    "TIPO_ACIDENTE",
    "FASE_DIA",
    "SENTIDO_VIA",
    "COND_METEOROLOGICA",
    "TIPO_PISTA",
    "ESTRUTURA_VIARIA",
    "LOCAL_URBANIZADO",
    "REGIAO",
]

NUMERIC_FEATURES = [
    "RODOVIA",
    "KM",
    "HORA",
    "FIM_DE_SEMANA",
    "HAS_IMAGE",
    "IMG_MEAN_BRIGHTNESS",
    "IMG_STD_BRIGHTNESS",
    "IMG_GREEN_RATIO",
    "IMG_ROAD_RATIO",
    "IMG_EDGE_DENSITY",
]


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"A variavel {name} nao foi configurada.")
    return value


def _sql_identifier(name: str) -> str:
    normalized = name.strip().upper()
    if not normalized.replace("_", "").isalnum():
        raise ValueError(f"Identificador Snowflake invalido: {name!r}")
    return normalized


def _extract_image_features(image_bytes: bytes) -> dict[str, float]:
    with Image.open(BytesIO(image_bytes)) as image:
        rgb = image.convert("RGB").resize((256, 256))
        pixels = np.asarray(rgb, dtype=np.float32)

    red = pixels[:, :, 0]
    green = pixels[:, :, 1]
    blue = pixels[:, :, 2]
    brightness = 0.299 * red + 0.587 * green + 0.114 * blue

    green_mask = (green > red * 1.08) & (green > blue * 1.08) & (green > 60)
    channel_spread = pixels.max(axis=2) - pixels.min(axis=2)
    road_mask = (channel_spread < 22) & (brightness > 55) & (brightness < 225)
    horizontal_edges = np.abs(np.diff(brightness, axis=1)) > 25
    vertical_edges = np.abs(np.diff(brightness, axis=0)) > 25

    return {
        "IMG_MEAN_BRIGHTNESS": float(brightness.mean()),
        "IMG_STD_BRIGHTNESS": float(brightness.std()),
        "IMG_GREEN_RATIO": float(green_mask.mean()),
        "IMG_ROAD_RATIO": float(road_mask.mean()),
        "IMG_EDGE_DENSITY": float(
            (horizontal_edges.mean() + vertical_edges.mean()) / 2
        ),
    }


def _load_dataset(
    connection,
    s3_client,
    bucket: str,
    generation: str,
    database: str,
) -> pd.DataFrame:
    accidents_table = f"{database}.INTERMEDIATE.INT_ACIDENTES"
    image_manifest = f"{database}.ML.IMAGE_MANIFEST"
    query = f"""
        SELECT
            a.CD_BAT,
            a.UF,
            a.RODOVIA,
            a.KM,
            a.CAUSA_ACIDENTE,
            a.TIPO_ACIDENTE,
            a.FASE_DIA,
            a.SENTIDO_VIA,
            a.COND_METEOROLOGICA,
            a.TIPO_PISTA,
            a.ESTRUTURA_VIARIA,
            a.LOCAL_URBANIZADO,
            a.HORA,
            a.DIA_SEMANA,
            a.FIM_DE_SEMANA,
            a.REGIAO,
            a.TARGET_COM_VITIMAS,
            m.S3_OBJECT_KEY
        FROM {accidents_table} a
        LEFT JOIN {image_manifest} m
          ON m.LATITUDE = a.LATITUDE
         AND m.LONGITUDE = a.LONGITUDE
         AND m.S3_BUCKET = %s
         AND m.STORAGE_GENERATION = %s
        WHERE a.TARGET_COM_VITIMAS IS NOT NULL
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query, (bucket, generation))
        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
    finally:
        cursor.close()

    dataframe = pd.DataFrame(rows, columns=columns)
    if dataframe.empty:
        raise RuntimeError("A tabela INTERMEDIATE.INT_ACIDENTES nao possui dados de treino.")

    dataframe["HAS_IMAGE"] = 0
    for column in (
        "IMG_MEAN_BRIGHTNESS",
        "IMG_STD_BRIGHTNESS",
        "IMG_GREEN_RATIO",
        "IMG_ROAD_RATIO",
        "IMG_EDGE_DENSITY",
    ):
        dataframe[column] = np.nan

    image_cache: dict[str, dict[str, float] | None] = {}
    for object_key in dataframe["S3_OBJECT_KEY"].dropna().unique():
        try:
            response = s3_client.get_object(Bucket=bucket, Key=str(object_key))
            image_cache[str(object_key)] = _extract_image_features(
                response["Body"].read()
            )
        except Exception as error:
            LOGGER.warning("Imagem %s ignorada: %s", object_key, error)
            image_cache[str(object_key)] = None

    for index, object_key in dataframe["S3_OBJECT_KEY"].items():
        if pd.isna(object_key):
            continue
        features = image_cache.get(str(object_key))
        if not features:
            continue
        dataframe.at[index, "HAS_IMAGE"] = 1
        for feature_name, feature_value in features.items():
            dataframe.at[index, feature_name] = feature_value

    return dataframe


def _build_model(random_seed: int) -> Pipeline:
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "one_hot",
                OneHotEncoder(
                    handle_unknown="ignore",
                    min_frequency=5,
                    sparse_output=True,
                ),
            ),
        ]
    )
    numeric_pipeline = Pipeline(
        steps=[("imputer", SimpleImputer(strategy="constant", fill_value=0.0))]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", categorical_pipeline, CATEGORICAL_FEATURES),
            ("numeric", numeric_pipeline, NUMERIC_FEATURES),
        ]
    )
    classifier = RandomForestClassifier(
        n_estimators=250,
        max_depth=30,
        min_samples_split=5,
        min_samples_leaf=2,
        class_weight="balanced",
        random_state=random_seed,
        n_jobs=-1,
    )
    return Pipeline(steps=[("preprocessor", preprocessor), ("classifier", classifier)])


def _persist_snowflake(
    connection,
    database: str,
    run: dict[str, Any],
    predictions: list[tuple],
    feature_importances: list[tuple],
) -> None:
    model_runs = f"{database}.ML.MODEL_RUNS"
    model_predictions = f"{database}.ML.MODEL_PREDICTIONS"
    feature_importance = f"{database}.ML.FEATURE_IMPORTANCE"
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {model_runs} (
                RUN_ID VARCHAR NOT NULL,
                MODEL_NAME VARCHAR NOT NULL,
                THRESHOLD FLOAT NOT NULL,
                RANDOM_SEED NUMBER NOT NULL,
                TRAIN_ROWS NUMBER NOT NULL,
                TEST_ROWS NUMBER NOT NULL,
                ROWS_WITH_IMAGE NUMBER NOT NULL,
                ACCURACY FLOAT,
                BALANCED_ACCURACY FLOAT,
                PRECISION_SCORE FLOAT,
                RECALL_SCORE FLOAT,
                F1_SCORE FLOAT,
                ROC_AUC FLOAT,
                TRUE_NEGATIVES NUMBER,
                FALSE_POSITIVES NUMBER,
                FALSE_NEGATIVES NUMBER,
                TRUE_POSITIVES NUMBER,
                STORAGE_GENERATION VARCHAR,
                ARTIFACT_S3_URI VARCHAR,
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {model_predictions} (
                RUN_ID VARCHAR NOT NULL,
                CD_BAT NUMBER NOT NULL,
                ACTUAL_TARGET NUMBER NOT NULL,
                PREDICTED_TARGET NUMBER NOT NULL,
                PROBABILITY_COM_VITIMAS FLOAT NOT NULL,
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {feature_importance} (
                RUN_ID VARCHAR NOT NULL,
                FEATURE_NAME VARCHAR NOT NULL,
                IMPORTANCE FLOAT NOT NULL,
                FEATURE_RANK NUMBER NOT NULL,
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        cursor.execute(
            f"""
            INSERT INTO {model_runs} (
                RUN_ID, MODEL_NAME, THRESHOLD, RANDOM_SEED, TRAIN_ROWS,
                TEST_ROWS, ROWS_WITH_IMAGE, ACCURACY, BALANCED_ACCURACY,
                PRECISION_SCORE, RECALL_SCORE, F1_SCORE, ROC_AUC,
                TRUE_NEGATIVES, FALSE_POSITIVES, FALSE_NEGATIVES,
                TRUE_POSITIVES, STORAGE_GENERATION, ARTIFACT_S3_URI
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                run["run_id"],
                MODEL_NAME,
                run["threshold"],
                run["random_seed"],
                run["train_rows"],
                run["test_rows"],
                run["rows_with_image"],
                run["accuracy"],
                run["balanced_accuracy"],
                run["precision"],
                run["recall"],
                run["f1"],
                run["roc_auc"],
                run["tn"],
                run["fp"],
                run["fn"],
                run["tp"],
                run["storage_generation"],
                run["artifact_s3_uri"],
            ),
        )
        cursor.executemany(
            f"""
            INSERT INTO {model_predictions} (
                RUN_ID, CD_BAT, ACTUAL_TARGET, PREDICTED_TARGET,
                PROBABILITY_COM_VITIMAS
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            predictions,
        )
        cursor.executemany(
            f"""
            INSERT INTO {feature_importance} (
                RUN_ID, FEATURE_NAME, IMPORTANCE, FEATURE_RANK
            ) VALUES (%s, %s, %s, %s)
            """,
            feature_importances,
        )
        connection.commit()
    finally:
        cursor.close()


def run_training_pipeline() -> dict[str, Any]:
    import boto3
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    bucket = _required_env("S3_BUCKET_NAME")
    region = _required_env("AWS_DEFAULT_REGION")
    database = _sql_identifier(_required_env("SNOWFLAKE_DATABASE"))
    threshold = float(os.getenv("ML_CLASSIFICATION_THRESHOLD", "0.37"))
    random_seed = int(os.getenv("ML_RANDOM_SEED", "42"))
    test_size = float(os.getenv("ML_TEST_SIZE", "0.25"))
    artifact_prefix = os.getenv("S3_ML_ARTIFACTS_PREFIX", "ml/artifacts").strip("/")

    s3_client = boto3.client("s3", region_name=region)
    generation = (
        s3_client.get_object(Bucket=bucket, Key=GENERATION_OBJECT_KEY)["Body"]
        .read()
        .decode("ascii")
        .strip()
    )
    connection = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()

    try:
        dataframe = _load_dataset(
            connection,
            s3_client,
            bucket,
            generation,
            database,
        )
        for column in CATEGORICAL_FEATURES:
            dataframe[column] = dataframe[column].fillna("Desconhecido").astype(str)
        for column in NUMERIC_FEATURES:
            dataframe[column] = pd.to_numeric(
                dataframe[column], errors="coerce"
            ).astype(float)
        dataframe[NUMERIC_FEATURES] = dataframe[NUMERIC_FEATURES].fillna(0.0)

        feature_columns = CATEGORICAL_FEATURES + NUMERIC_FEATURES
        features = dataframe[feature_columns]
        target = dataframe["TARGET_COM_VITIMAS"].astype(int)

        train_indices, test_indices = train_test_split(
            dataframe.index,
            test_size=test_size,
            random_state=random_seed,
            stratify=target,
        )
        model = _build_model(random_seed)
        model.fit(features.loc[train_indices], target.loc[train_indices])

        probabilities = model.predict_proba(features.loc[test_indices])[:, 1]
        predictions_array = (probabilities >= threshold).astype(int)
        actual = target.loc[test_indices].to_numpy()
        tn, fp, fn, tp = confusion_matrix(actual, predictions_array).ravel()

        run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"
        artifact_key = f"{artifact_prefix}/{run_id}/model.joblib"
        artifact_buffer = BytesIO()
        joblib.dump(model, artifact_buffer)
        artifact_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket,
            Key=artifact_key,
            Body=artifact_buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        run = {
            "run_id": run_id,
            "threshold": threshold,
            "random_seed": random_seed,
            "train_rows": int(len(train_indices)),
            "test_rows": int(len(test_indices)),
            "rows_with_image": int(dataframe["HAS_IMAGE"].sum()),
            "accuracy": float(accuracy_score(actual, predictions_array)),
            "balanced_accuracy": float(
                balanced_accuracy_score(actual, predictions_array)
            ),
            "precision": float(
                precision_score(actual, predictions_array, zero_division=0)
            ),
            "recall": float(recall_score(actual, predictions_array, zero_division=0)),
            "f1": float(f1_score(actual, predictions_array, zero_division=0)),
            "roc_auc": float(roc_auc_score(actual, probabilities)),
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
            "storage_generation": generation,
            "artifact_s3_uri": f"s3://{bucket}/{artifact_key}",
        }

        metrics_key = f"{artifact_prefix}/{run_id}/metrics.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=metrics_key,
            Body=json.dumps(run, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        prediction_rows = [
            (
                run_id,
                int(dataframe.loc[index, "CD_BAT"]),
                int(actual_value),
                int(predicted_value),
                float(probability),
            )
            for index, actual_value, predicted_value, probability in zip(
                test_indices, actual, predictions_array, probabilities
            )
        ]

        preprocessor = model.named_steps["preprocessor"]
        feature_names = preprocessor.get_feature_names_out()
        importances = model.named_steps["classifier"].feature_importances_
        ordered = sorted(
            zip(feature_names, importances), key=lambda item: item[1], reverse=True
        )
        importance_rows = [
            (run_id, str(name), float(importance), rank)
            for rank, (name, importance) in enumerate(ordered, start=1)
        ]

        _persist_snowflake(
            connection,
            database,
            run,
            prediction_rows,
            importance_rows,
        )
    finally:
        connection.close()

    LOGGER.info(
        "Treino %s concluido: F1=%.4f, AUC=%.4f, imagens=%s.",
        run_id,
        run["f1"],
        run["roc_auc"],
        run["rows_with_image"],
    )
    return run
