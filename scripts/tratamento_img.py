"""
Instalação:
    pip install requests pandas pillow opencv-python-headless scikit-image pyarrow --break-system-packages
"""

import os
import sys
import time
import math
from datetime import datetime
from pathlib import Path
from io import BytesIO

import cv2
import requests
import numpy as np
import pandas as pd
from PIL import Image
from skimage import color, feature

# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

MAPBOX_TOKEN = os.getenv("MAPBOX_ACCESS_TOKEN", "pk.eyJ1IjoiYW5hYnBhZXNzIiwiYSI6ImNtcm85bXNybzA3NTgzNm9sb2k0d2s1amYifQ.mBz5IYviUCsXroqcjTajDg")
MAPBOX_STYLE = "mapbox/streets-v12"
IMG_WIDTH = 400
IMG_HEIGHT = 300
ZOOM = 17 

BASE_DIR = Path("image_lab")
RAW_DIR = BASE_DIR / "raw"
FEATURES_DIR = BASE_DIR / "features"
RAW_DIR.mkdir(parents=True, exist_ok=True)
FEATURES_DIR.mkdir(parents=True, exist_ok=True)

REQUEST_DELAY_SEC = 0.2

# ------------------------------------------------------------
# AMOSTRAGEM / LOTES - lê a planilha de sinistros inteira e limita
# quantos locais são de fato consultados na API nesta execução, sem
# precisar gerar um CSV separado a cada teste.
#
# A ordem é embaralhada uma única vez com RANDOM_SEED fixo, então os
# lotes nunca se sobrepõem entre execuções diferentes.
# ------------------------------------------------------------
LIMITE_LOCAIS = 50      # quantos locais processar nesta execução (None = todos)
LOTE_INICIO = 0         # posição inicial do lote (para pular locais já processados)
RANDOM_SEED = 42        # fixa a ordem de embaralhamento (reprodutibilidade)

# ------------------------------------------------------------
# LIMIARES DE NEGÓCIO 
# ------------------------------------------------------------

CANNY_LOW, CANNY_HIGH = 50, 150      
HOUGH_THRESHOLD = 40            
HOUGH_MIN_LINE_LEN = 25          
HOUGH_MAX_LINE_GAP = 15          
ANGLE_DIFF_MIN_DEG = 25    
CURVA_STD_ANGLE_THRESHOLD_DEG = 20
CRUZAMENTO_MIN_INTERSECOES = 1           

LINE_CLUSTER_ANGLE_DEG = 12             
LINE_CLUSTER_RHO_PX = 20                 

VEGETACAO_HUE_RANGE = (35, 85)
VEGETACAO_SAT_MIN = 40
VEGETACAO_VAL_MIN = 40

BUILDING_SAT_MAX = 35          
BUILDING_VALUE_DELTA_MIN = 6    
BUILDING_VALUE_DELTA_MAX = 55    

PCT_AREA_CONSTRUIDA_ALTO = 12.0  
PCT_VEGETACAO_ALTO = 30.0       

LOW_LIGHT_BRIGHTNESS_THRESHOLD = 90.0 

# ============================================================
# ETAPA 1 - CONSULTA À API E SALVAMENTO NA CAMADA RAW
# ============================================================

def buscar_imagem(lat: float, lon: float, local_id) -> Path:
    url = (
        f"https://api.mapbox.com/styles/v1/{MAPBOX_STYLE}/static/"
        f"{lon},{lat},{ZOOM},0,0/{IMG_WIDTH}x{IMG_HEIGHT}"
        f"?access_token={MAPBOX_TOKEN}"
    )
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()

    img = Image.open(BytesIO(resp.content)).convert("RGB")
    file_path = RAW_DIR / f"{local_id}.png"
    img.save(file_path)
    return file_path

# ============================================================
# ETAPA 2 - METADADOS
# ============================================================

def extract_image_metadata(file_path: Path) -> dict:
    img = Image.open(file_path)
    return {
        "image_id": file_path.stem,
        "file_name": file_path.name,
        "file_path": str(file_path),
        "format": img.format,
        "width": img.width,
        "height": img.height,
        "mode": img.mode,
        "file_size_bytes": file_path.stat().st_size,
        "metadata_extracted_at": datetime.now().isoformat(timespec="seconds"),
    }

# ============================================================
# ETAPA 3A - ATRIBUTOS BÁSICOS (brilho, contraste, cor, bordas)
# ============================================================

def extract_basic_features(img_rgb: np.ndarray) -> dict:
    img_gray = color.rgb2gray(img_rgb)
    height, width, channels = img_rgb.shape

    brightness_mean = float(np.mean(img_gray)) * 255 
    brightness_std = float(np.std(img_gray)) * 255
    contrast = float(img_gray.max() - img_gray.min()) * 255

    red_mean = float(np.mean(img_rgb[:, :, 0]))
    green_mean = float(np.mean(img_rgb[:, :, 1]))
    blue_mean = float(np.mean(img_rgb[:, :, 2]))

    aspect_ratio = float(width / height)

    edges = feature.canny(img_gray, sigma=1)
    edge_count = int(np.sum(edges))
    edge_density = float(edge_count / (width * height))

    return {
        "width": width,
        "height": height,
        "channels": channels,
        "aspect_ratio": aspect_ratio,
        "brightness_mean": brightness_mean,
        "brightness_std": brightness_std,
        "contrast": contrast,
        "red_mean": red_mean,
        "green_mean": green_mean,
        "blue_mean": blue_mean,
        "edge_count": edge_count,
        "edge_density": edge_density,
    }

# ============================================================
# ETAPA 3B - RETAS E INTERSEÇÕES (Transformada de Hough)
# ============================================================

def _line_intersection(seg1, seg2, width, height):
    x1, y1, x2, y2 = seg1
    x3, y3, x4, y4 = seg2

    denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if denom == 0:
        return None  

    px = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / denom
    py = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / denom

    if 0 <= px <= width and 0 <= py <= height:
        return (px, py)
    return None


def _line_normal_form(seg, angle_deg):
    x1, y1, _, _ = seg
    normal_rad = math.radians(angle_deg + 90)
    rho = x1 * math.cos(normal_rad) + y1 * math.sin(normal_rad)
    return rho

def _cluster_lines(segs, angles):
    rhos = [_line_normal_form(s, a) for s, a in zip(segs, angles)]

    clusters, used = [], [False] * len(segs)
    for i in range(len(segs)):
        if used[i]:
            continue
        grupo = [i]
        used[i] = True
        for j in range(i + 1, len(segs)):
            if used[j]:
                continue
            diff_angle = abs(angles[i] - angles[j])
            diff_angle = min(diff_angle, 180 - diff_angle)
            diff_rho = abs(rhos[i] - rhos[j])
            if diff_angle <= LINE_CLUSTER_ANGLE_DEG and diff_rho <= LINE_CLUSTER_RHO_PX:
                grupo.append(j)
                used[j] = True
        clusters.append(grupo)

    reps_segs, reps_angles = [], []
    for grupo in clusters:
        comprimentos = [
            math.hypot(segs[k][2] - segs[k][0], segs[k][3] - segs[k][1]) for k in grupo
        ]
        melhor = grupo[int(np.argmax(comprimentos))]
        reps_segs.append(segs[melhor])
        reps_angles.append(angles[melhor])

    return reps_segs, reps_angles


def extract_line_features(img_gray_uint8: np.ndarray, width: int, height: int) -> dict:
    edges = cv2.Canny(img_gray_uint8, CANNY_LOW, CANNY_HIGH)
    lines = cv2.HoughLinesP(
        edges,
        rho=1,
        theta=np.pi / 180,
        threshold=HOUGH_THRESHOLD,
        minLineLength=HOUGH_MIN_LINE_LEN,
        maxLineGap=HOUGH_MAX_LINE_GAP,
    )

    if lines is None:
        return {
            "line_count": 0,
            "line_angle_mean": 0.0,
            "line_angle_std": 0.0,
            "line_intersection_count": 0,
        }

    lines = np.asarray(lines).reshape(-1, 4)

    segs_brutos, angles_brutos = [], []
    for x1, y1, x2, y2 in lines:
        angle = math.degrees(math.atan2(int(y2) - int(y1), int(x2) - int(x1))) % 180
        segs_brutos.append((int(x1), int(y1), int(x2), int(y2)))
        angles_brutos.append(angle)

    segs, angles = _cluster_lines(segs_brutos, angles_brutos)

    intersection_count = 0
    for i in range(len(segs)):
        for j in range(i + 1, len(segs)):
            diff = abs(angles[i] - angles[j])
            diff = min(diff, 180 - diff)
            if diff >= ANGLE_DIFF_MIN_DEG:
                if _line_intersection(segs[i], segs[j], width, height) is not None:
                    intersection_count += 1

    return {
        "line_count": len(segs),
        "line_angle_mean": float(np.mean(angles)),
        "line_angle_std": float(np.std(angles)),
        "line_intersection_count": intersection_count,
    }

# ============================================================
# ETAPA 3C - SEGMENTAÇÃO DE COR EM HSV (vegetação, céu, asfalto)
# ============================================================

def _cor_dominante_fundo_hsv(img_hsv: np.ndarray) -> tuple:
    h, s, v = img_hsv[:, :, 0], img_hsv[:, :, 1], img_hsv[:, :, 2]
    h_q = (h // 4) * 4
    s_q = (s // 8) * 8
    v_q = (v // 8) * 8

    combinados = np.stack([h_q, s_q, v_q], axis=-1).reshape(-1, 3)
    valores, contagens = np.unique(combinados, axis=0, return_counts=True)
    idx_mais_comum = int(np.argmax(contagens))
    return tuple(int(x) for x in valores[idx_mais_comum])


def extract_color_segmentation_features(img_rgb: np.ndarray) -> dict:
    img_hsv = cv2.cvtColor(img_rgb, cv2.COLOR_RGB2HSV)
    h, s, v = img_hsv[:, :, 0], img_hsv[:, :, 1], img_hsv[:, :, 2]
    total_pixels = h.size

    bg_h, bg_s, bg_v = _cor_dominante_fundo_hsv(img_hsv)

    building_mask = (
        (s <= BUILDING_SAT_MAX)
        & (v <= bg_v - BUILDING_VALUE_DELTA_MIN)
        & (v >= bg_v - BUILDING_VALUE_DELTA_MAX)
    )

    veg_mask = (
        (h >= VEGETACAO_HUE_RANGE[0])
        & (h <= VEGETACAO_HUE_RANGE[1])
        & (s >= VEGETACAO_SAT_MIN)
        & (v >= VEGETACAO_VAL_MIN)
    )

    return {
        "bg_hue": bg_h,
        "bg_sat": bg_s,
        "bg_val": bg_v,
        "pct_area_construida": float(np.sum(building_mask) / total_pixels * 100),
        "pct_vegetacao": float(np.sum(veg_mask) / total_pixels * 100),
    }

# ============================================================
# ETAPA 3D - TEXTURA (GLCM - Matriz de Coocorrência de Níveis de Cinza)
# ============================================================

def extract_texture_features(img_gray_uint8: np.ndarray, levels: int = 8) -> dict:
    img_quantized = (img_gray_uint8 / 256 * levels).astype(np.uint8)

    glcm = feature.graycomatrix(
        img_quantized,
        distances=[1],
        angles=[0],
        levels=levels,
        symmetric=True,
        normed=True,
    )

    return {
        "texture_contrast": float(feature.graycoprops(glcm, "contrast")[0, 0]),
        "texture_homogeneity": float(feature.graycoprops(glcm, "homogeneity")[0, 0]),
        "texture_energy": float(feature.graycoprops(glcm, "energy")[0, 0]),
        "texture_correlation": float(feature.graycoprops(glcm, "correlation")[0, 0]),
    }

# ============================================================
# ETAPA 4 - DERIVAÇÃO DE ATRIBUTOS DE RISCO 
# ============================================================

def derivar_atributos_risco(row: pd.Series) -> dict:
    possui_cruzamento = row["line_intersection_count"] >= CRUZAMENTO_MIN_INTERSECOES
    curva_via = row["line_angle_std"] >= CURVA_STD_ANGLE_THRESHOLD_DEG

    if row["pct_area_construida"] >= PCT_AREA_CONSTRUIDA_ALTO:
        tipo_via = "urbana_com_edificacoes"
    elif possui_cruzamento:
        tipo_via = "urbana_com_cruzamento"
    elif row["pct_vegetacao"] >= PCT_VEGETACAO_ALTO:
        tipo_via = "aberta_rural_arborizada"
    else:
        tipo_via = "via_simples"

    low_light_flag = row["brightness_mean"] < LOW_LIGHT_BRIGHTNESS_THRESHOLD

    return {
        "possui_cruzamento": possui_cruzamento,
        "curva_via": curva_via,
        "tipo_via": tipo_via,
        "low_light_flag": low_light_flag,
    }

# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def processar_imagem_completa(file_path: Path) -> dict:
    img_pil = Image.open(file_path).convert("RGB")
    img_rgb = np.array(img_pil)
    img_gray_uint8 = cv2.cvtColor(img_rgb, cv2.COLOR_RGB2GRAY)
    height, width = img_gray_uint8.shape

    atributos = {"image_id": file_path.stem}
    atributos.update(extract_basic_features(img_rgb))
    atributos.update(extract_line_features(img_gray_uint8, width, height))
    atributos.update(extract_color_segmentation_features(img_rgb))
    atributos.update(extract_texture_features(img_gray_uint8))
    atributos["features_extracted_at"] = datetime.now().isoformat(timespec="seconds")
    return atributos

def _ler_csv_com_fallback_encoding(caminho_csv: str) -> pd.DataFrame:
    for encoding in ("utf-8", "latin-1"):
        for sep in (",", ";"):
            try:
                df = pd.read_csv(caminho_csv, encoding=encoding, sep=sep)
                if {"id", "lat", "lon"}.issubset(df.columns) or len(df.columns) > 1:
                    print(f"CSV lido com encoding='{encoding}', separador='{sep}'")
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
    raise ValueError(
        "Não foi possível ler o CSV com UTF-8/latin-1 nem separador ','/';'. "
        "Verifique se o arquivo de entrada é o 'locais_teste.csv' "
        "(gerado por gerar_amostra_locais.py), não o CSV bruto da PRF."
    )


def _padronizar_colunas_local(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "lat" not in df.columns and "latitude" in df.columns:
        df = df.rename(columns={"latitude": "lat", "longitude": "lon"})

    for col in ("lat", "lon"):
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = (
                df[col].astype(str).str.replace(",", ".", regex=False).astype(float)
            )
    return df


def processar_locais(caminho_csv_entrada: str) -> pd.DataFrame:
    df_bruto = _ler_csv_com_fallback_encoding(caminho_csv_entrada)
    df_bruto = _padronizar_colunas_local(df_bruto)

    colunas_necessarias = {"id", "lat", "lon"}
    faltantes = colunas_necessarias - set(df_bruto.columns)
    if faltantes:
        raise ValueError(f"CSV de entrada sem as colunas obrigatórias: {faltantes}")

    df_validos = df_bruto.dropna(subset=["lat", "lon"])
    df_unicos = df_validos.drop_duplicates(subset=["lat", "lon"]).reset_index(drop=True)

    df_embaralhado = df_unicos.sample(frac=1, random_state=RANDOM_SEED).reset_index(drop=True)

    fim_lote = None if LIMITE_LOCAIS is None else LOTE_INICIO + LIMITE_LOCAIS
    df_locais = df_embaralhado.iloc[LOTE_INICIO:fim_lote].reset_index(drop=True)

    print(f"Base com {len(df_unicos)} locais únicos disponíveis no CSV.")
    print(
        f"Processando lote: posições {LOTE_INICIO} a "
        f"{LOTE_INICIO + len(df_locais) - 1} ({len(df_locais)} locais nesta execução).\n"
    )

    metadata_rows, feature_rows = [], []
    total = len(df_locais)

    for i, row in df_locais.iterrows():
        local_id, lat, lon = row["id"], row["lat"], row["lon"]
        try:
            file_path = buscar_imagem(lat, lon, local_id)
            metadata_rows.append(extract_image_metadata(file_path))
            feature_rows.append(processar_imagem_completa(file_path))
            print(f"[{i + 1}/{total}] OK - local {local_id}")
        except Exception as e:
            print(f"[{i + 1}/{total}] ERRO - local {local_id}: {e}")
        time.sleep(REQUEST_DELAY_SEC)

    if not feature_rows:
        print("Nenhum local processado com sucesso.")
        return pd.DataFrame()

    metadata_df = pd.DataFrame(metadata_rows)
    features_df = pd.DataFrame(feature_rows)

    image_dataset_df = metadata_df.merge(
        features_df, on=["image_id", "width", "height"], how="inner"
    )

    df_locais_renomeado = df_locais.rename(columns={"id": "image_id"})
    df_locais_renomeado["image_id"] = df_locais_renomeado["image_id"].astype(str)
    image_dataset_df["image_id"] = image_dataset_df["image_id"].astype(str)
    image_dataset_df = image_dataset_df.merge(
        df_locais_renomeado[["image_id", "lat", "lon"]], on="image_id", how="left"
    )

    risco_df = image_dataset_df.apply(derivar_atributos_risco, axis=1, result_type="expand")
    image_dataset_df = pd.concat([image_dataset_df, risco_df], axis=1)

    csv_path = FEATURES_DIR / "atributos_locais.csv"
    parquet_path = FEATURES_DIR / "atributos_locais.parquet"
    image_dataset_df.to_csv(csv_path, index=False)
    image_dataset_df.to_parquet(parquet_path, index=False)

    print(f"\nCSV salvo em: {csv_path}")
    print(f"Parquet salvo em: {parquet_path}")
    print(f"Total de registros: {len(image_dataset_df)}")

    return image_dataset_df

if __name__ == "__main__":
    caminho_entrada = sys.argv[1] if len(sys.argv) > 1 else "datatran2026.csv"
    processar_locais(caminho_entrada)