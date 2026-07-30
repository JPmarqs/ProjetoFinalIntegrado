#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
PIPELINE BINÁRIO COM THRESHOLD AJUSTÁVEL - PREVISÃO DE ACIDENTES COM VÍTIMAS (PRF)
=============================================================================
Versão com threshold configurável (default: 0.37)
Target: 1 = Com Vítimas (feridos ou fatais) | 0 = Sem Vítimas
Modelo: Random Forest + SMOTE
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import cv2
import os
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# 1. CONFIGURAÇÕES
# =============================================================================

CSV_PATH = 'datatran2026.csv'           # <-- ALTERE PARA O CAMINHO DO SEU ARQUIVO
THRESHOLD = 0.37                      # <-- AJUSTE O THRESHOLD AQUI (0.0 a 1.0)

# Imagens de mapa: use a coluna `caminho_imagem` no CSV ou os arquivos em
# ../mapas com o padrão mapa_<id>.png (por exemplo, ../mapas/mapa_123.png).
IMAGENS_DIR = os.path.join('..', 'mapas')
COLUNA_CAMINHO_IMAGEM = 'caminho_imagem'
COLUNA_ID_IMAGEM = 'id'
PREFIXO_IMAGEM = 'mapa_'
EXTENSAO_IMAGEM = '.png'

# Features extraídas das imagens
IMG_RESIZE_SIZE = (256, 256)
COLUNAS_IMAGEM = ['prop_via_principal', 'prop_vegetacao']

# Laranja (ex.: Rodovia Presidente Vargas)
FAIXA_VIA_LARANJA = ((0, 70, 180), (15, 180, 255))
# Rosa/magenta (ex.: Rodovia Presidente Dutra)
FAIXA_VIA_ROSA = ((165, 40, 180), (180, 180, 255))
FAIXA_VEGETACAO = ((35, 40, 40), (85, 255, 255))


def extrair_features_imagem(caminho_imagem: str) -> np.ndarray:
    """Extrai proporções de via principal e vegetação de uma imagem de mapa."""
    if not caminho_imagem or not os.path.exists(caminho_imagem):
        return np.zeros(len(COLUNAS_IMAGEM), dtype=np.float32)

    img_bgr = cv2.imread(caminho_imagem)
    if img_bgr is None:
        return np.zeros(len(COLUNAS_IMAGEM), dtype=np.float32)

    img_bgr = cv2.resize(img_bgr, IMG_RESIZE_SIZE)
    img_hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
    total_pixels = IMG_RESIZE_SIZE[0] * IMG_RESIZE_SIZE[1]

    mascara_laranja = cv2.inRange(
        img_hsv, np.array(FAIXA_VIA_LARANJA[0]), np.array(FAIXA_VIA_LARANJA[1])
    )
    mascara_rosa = cv2.inRange(
        img_hsv, np.array(FAIXA_VIA_ROSA[0]), np.array(FAIXA_VIA_ROSA[1])
    )
    mascara_via_principal = cv2.bitwise_or(mascara_laranja, mascara_rosa)
    prop_via_principal = float(np.count_nonzero(mascara_via_principal)) / total_pixels

    mascara_vegetacao = cv2.inRange(
        img_hsv, np.array(FAIXA_VEGETACAO[0]), np.array(FAIXA_VEGETACAO[1])
    )
    prop_vegetacao = float(np.count_nonzero(mascara_vegetacao)) / total_pixels

    return np.array([prop_via_principal, prop_vegetacao], dtype=np.float32)


def obter_caminho_imagem(linha: pd.Series) -> str:
    """Prioriza a coluna de caminho; sem ela, monta ../mapas/mapa_<id>.png."""
    caminho = linha.get(COLUNA_CAMINHO_IMAGEM)
    if pd.notna(caminho) and str(caminho).strip():
        return str(caminho).strip()

    identificador = linha.get(COLUNA_ID_IMAGEM)
    if pd.isna(identificador):
        return ''
    return os.path.join(IMAGENS_DIR, f'{PREFIXO_IMAGEM}{identificador}{EXTENSAO_IMAGEM}')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
np.random.seed(42)

print("=" * 70)
print("PIPELINE BINÁRIO - PREVISÃO DE ACIDENTES COM VÍTIMAS (PRF)")
print(f"Threshold definido: {THRESHOLD}")
print("=" * 70)

# =============================================================================
# 2. CARREGAMENTO DOS DADOS
# =============================================================================

print("\\n[1/6] Carregando dados...")

df = pd.read_csv(CSV_PATH, sep=';', encoding='utf-8-sig', low_memory=False)

print(f"✓ Dataset carregado: {df.shape[0]:,} linhas x {df.shape[1]} colunas")

# =============================================================================
# 3. LIMPEZA E FEATURE ENGINEERING
# =============================================================================

print("\\n[2/6] Limpeza e Feature Engineering...")

df_model = df.copy()

# Limpar target
df_model['classificacao_acidente'] = df_model['classificacao_acidente'].str.strip()
df_model = df_model[df_model['classificacao_acidente'].notna()]
df_model = df_model[df_model['classificacao_acidente'] != 'NA']
df_model = df_model[df_model['classificacao_acidente'] != '']

# Normalizar classificação (lida com encoding corrompido)
def normalizar_classificacao(valor):
    valor = str(valor).strip().lower()
    if 'fatal' in valor or 'mort' in valor:
        return 'Com Vítimas Fatais'
    elif 'ferid' in valor or 'les' in valor:
        return 'Com Vítimas Feridas'
    elif 'sem' in valor or 'nenhum' in valor or 'iles' in valor:
        return 'Sem Vítimas'
    else:
        return 'Outro'

df_model['classificacao_acidente_norm'] = df_model['classificacao_acidente'].apply(normalizar_classificacao)

# --- Feature Engineering ---
df_model['hora'] = pd.to_datetime(df_model['horario'], format='%H:%M:%S', errors='coerce').dt.hour

def faixa_horaria(hora):
    if pd.isna(hora): return 'Desconhecido'
    elif 0 <= hora < 6: return 'Madrugada'
    elif 6 <= hora < 12: return 'Manhã'
    elif 12 <= hora < 18: return 'Tarde'
    else: return 'Noite'

df_model['faixa_horaria'] = df_model['hora'].apply(faixa_horaria)

df_model['fim_de_semana'] = df_model['dia_semana'].apply(
    lambda x: 'Sim' if str(x).lower() in ['sábado', 'sabado', 'domingo'] else 'Não'
)

regioes = {
    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 
    'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
}
df_model['regiao'] = df_model['uf'].map(regioes).fillna('Desconhecida')

def agrupar_causa(causa):
    causa = str(causa).lower()
    if any(p in causa for p in ['velocidade', 'ultrapass', 'distância', 'distancia']):
        return 'Velocidade/Ultrapassagem'
    elif any(p in causa for p in ['dormir', 'sono', 'fadiga']):
        return 'Fadiga/Sono'
    elif any(p in causa for p in ['álcool', 'alcool', 'drog', 'embriag']):
        return 'Álcool/Drogas'
    elif any(p in causa for p in ['chuva', 'molha', 'escorreg', 'pista']):
        return 'Condição da Pista'
    elif any(p in causa for p in ['defeito', 'falha', 'pneu', 'mecânica', 'mecanica']):
        return 'Defeito Mecânico'
    elif any(p in causa for p in ['animal', 'pedestre', 'objeto']):
        return 'Obstáculo na Via'
    elif any(p in causa for p in ['não guardar', 'nao guardar', 'preferência', 'preferencia', 'sinal']):
        return 'Desrespeito às Normas'
    elif any(p in causa for p in ['inexperiência', 'inexperiencia', 'habilidade']):
        return 'Inexperiência'
    elif any(p in causa for p in ['celular', 'distração', 'distraçao', 'atenção', 'atencao']):
        return 'Distração'
    else:
        return 'Outras'

df_model['causa_agrupada'] = df_model['causa_acidente'].apply(agrupar_causa)

def simplificar_tracado(tracado):
    tracado = str(tracado).lower()
    if 'reta' in tracado and 'curva' not in tracado: return 'Reta'
    elif 'curva' in tracado and 'reta' not in tracado: return 'Curva'
    elif 'reta' in tracado and 'curva' in tracado: return 'Misto'
    elif 'interse' in tracado or 'cruz' in tracado: return 'Interseção'
    elif 'rot' in tracado: return 'Rotatória'
    elif 'ponte' in tracado or 'viadu' in tracado: return 'Ponte/Viaduto'
    else: return 'Outro'

df_model['tracado_simplificado'] = df_model['tracado_via'].apply(simplificar_tracado)

df_model['horario_perigoso'] = df_model['hora'].apply(
    lambda x: 'Sim' if pd.notna(x) and (x < 6 or x > 22) else 'Não'
)

print(f"✓ {df_model.shape[0]:,} registros válidos")

# =============================================================================
# 4. PREPARAÇÃO DO TARGET BINÁRIO E FEATURES
# =============================================================================

print("\\n[3/6] Preparando target binário e features...")

y_bin = df_model['classificacao_acidente_norm'].apply(lambda x: 0 if x == 'Sem Vítimas' else 1)

print(f"\\nDistribuição do target binário:")
print(y_bin.value_counts())
print(f"Percentuais: {(y_bin.value_counts(normalize=True) * 100).round(2).to_dict()}")

features_categoricas = [
    'dia_semana', 'uf', 'causa_agrupada', 'tipo_acidente',
    'fase_dia', 'sentido_via', 'condicao_metereologica',
    'tipo_pista', 'uso_solo', 'faixa_horaria', 'fim_de_semana',
    'regiao', 'tracado_simplificado', 'horario_perigoso'
]

X_categoricas = df_model[features_categoricas].copy().fillna('Desconhecido')
X_categoricas_encoded = pd.get_dummies(X_categoricas, drop_first=False)

# Para cada acidente, extraia as duas proporções a partir do mapa associado.
# Quando não houver imagem, extrair_features_imagem devolve zeros, mantendo a
# mesma estrutura de features no treino e na inferência.
caminhos_imagem = df_model.apply(obter_caminho_imagem, axis=1)
qtd_imagens_encontradas = caminhos_imagem.map(os.path.exists).sum()
features_imagem = np.vstack(caminhos_imagem.map(extrair_features_imagem).to_numpy())
X_imagem = pd.DataFrame(features_imagem, columns=COLUNAS_IMAGEM, index=df_model.index)

X_encoded = pd.concat([X_categoricas_encoded, X_imagem], axis=1).astype(np.float64)

print(f"\\nFeatures após encoding: {X_encoded.shape[1]} colunas")
print(f"Features de imagem: {COLUNAS_IMAGEM} | imagens encontradas: "
      f"{qtd_imagens_encontradas:,}/{len(df_model):,}")

# =============================================================================
# 5. DIVISÃO TREINO/TESTE E SMOTE
# =============================================================================

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score

print("\\n[4/6] Dividindo dados e aplicando SMOTE...")

X_train, X_test, y_train, y_test = train_test_split(
    X_encoded, y_bin, test_size=0.25, random_state=42, stratify=y_bin
)

print(f"Treino: {X_train.shape[0]:,} | Teste: {X_test.shape[0]:,}")

from imblearn.over_sampling import SMOTE

smote = SMOTE(random_state=42, k_neighbors=5)
X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

print(f"\\nAntes do SMOTE: {pd.Series(y_train).value_counts().to_dict()}")
print(f"Depois do SMOTE: {pd.Series(y_train_smote).value_counts().to_dict()}")

# =============================================================================
# 6. TREINAMENTO DO MODELO
# =============================================================================

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                            f1_score, classification_report, confusion_matrix,
                            roc_auc_score, roc_curve, balanced_accuracy_score)
import time

print("\\n[5/6] Treinando Random Forest...")

start = time.time()

rf = RandomForestClassifier(
    n_estimators=300,
    max_depth=30,
    min_samples_split=5,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=-1
)

rf.fit(X_train_smote, y_train_smote)

tempo_treino = time.time() - start
print(f"✓ Treinamento concluído em {tempo_treino:.2f}s")

# =============================================================================
# 7. AVALIAÇÃO COM THRESHOLD AJUSTÁVEL
# =============================================================================

print("\\n[6/6] Avaliando modelo...")

# Probabilidades
y_proba = rf.predict_proba(X_test)[:, 1]

# Aplicar threshold customizado
y_pred = (y_proba >= THRESHOLD).astype(int)

# Métricas principais
print("\\n" + "=" * 70)
print(f"MÉTRICAS DE PERFORMANCE (Threshold = {THRESHOLD})")
print("=" * 70)
print(f"Accuracy:     {accuracy_score(y_test, y_pred):.4f}")
print(f"Balanced Acc: {balanced_accuracy_score(y_test, y_pred):.4f}")
print(f"Precision:    {precision_score(y_test, y_pred):.4f}")
print(f"Recall:       {recall_score(y_test, y_pred):.4f}")
print(f"F1-Score:     {f1_score(y_test, y_pred):.4f}")
print(f"AUC-ROC:      {roc_auc_score(y_test, y_proba):.4f}")

# Cross-validation (usa threshold padrão 0.5, mas modelo é o mesmo)
print("\\n--- Cross-Validation (5 folds) ---")
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
cv_scores = cross_val_score(rf, X_train_smote, y_train_smote, cv=cv, scoring='f1')
print(f"F1-Score médio (CV): {cv_scores.mean():.4f} (+/- {cv_scores.std()*2:.4f})")

# Classification Report
print("\\n--- Classification Report ---")
print(classification_report(y_test, y_pred, target_names=['Sem Vítimas', 'Com Vítimas']))

# Matriz de Confusão
print("\\n--- Matriz de Confusão ---")
cm = confusion_matrix(y_test, y_pred)
print(pd.DataFrame(cm, 
                   index=['Real: Sem Vítimas', 'Real: Com Vítimas'],
                   columns=['Pred: Sem Vítimas', 'Pred: Com Vítimas']))

# Análise de impacto
vp = ((y_test == 1) & (y_pred == 1)).sum()
vn = ((y_test == 0) & (y_pred == 0)).sum()
fp = ((y_test == 0) & (y_pred == 1)).sum()
fn = ((y_test == 1) & (y_pred == 0)).sum()

print("\\n" + "=" * 70)
print(f"ANÁLISE DE IMPACTO PRÁTICO (Threshold = {THRESHOLD})")
print("=" * 70)
print(f"Total de acidentes no teste: {len(y_test):,}")
print(f"\\n✓ Vítimas CORRETAMENTE atendidas:       {vp:,} ({vp/(vp+fn)*100:.1f}%)")
print(f"✓ Não-despachos corretos:              {vn:,}")
print(f"⚠ Despachos DESNECESSÁRIOS:            {fp:,} ({fp/(fp+vn)*100:.1f}%)")
print(f"✗ Vítimas NÃO atendidas:               {fn:,} ({fn/(vp+fn)*100:.1f}%)")

# =============================================================================
# 8. COMPARAÇÃO DE THRESHOLDS
# =============================================================================

print("\\n" + "=" * 70)
print("COMPARAÇÃO DE THRESHOLDS")
print("=" * 70)

thresholds_teste = [0.3, 0.37, 0.4, 0.5, 0.6, 0.7]
print(f"{'Threshold':>10} | {'Accuracy':>8} | {'Precision':>9} | {'Recall':>6} | {'F1':>6} | {'FN':>6} | {'FP':>6}")
print("-" * 70)

for t in thresholds_teste:
    y_p = (y_proba >= t).astype(int)
    acc = accuracy_score(y_test, y_p)
    prec = precision_score(y_test, y_p)
    rec = recall_score(y_test, y_p)
    f1 = f1_score(y_test, y_p)
    fn_t = ((y_test == 1) & (y_p == 0)).sum()
    fp_t = ((y_test == 0) & (y_p == 1)).sum()
    marker = " <--" if t == THRESHOLD else ""
    print(f"{t:>10.2f} | {acc:>8.4f} | {prec:>9.4f} | {rec:>6.4f} | {f1:>6.4f} | {fn_t:>6} | {fp_t:>6}{marker}")

# =============================================================================
# 9. VISUALIZAÇÕES
# =============================================================================

print("\\nGerando visualizações...")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle(f'Resultados - Modelo Binário (Threshold = {THRESHOLD})', 
             fontsize=14, fontweight='bold')

# 1. Curva ROC
fpr, tpr, _ = roc_curve(y_test, y_proba)
axes[0, 0].plot(fpr, tpr, color='darkorange', lw=2, 
                label=f'ROC (AUC = {roc_auc_score(y_test, y_proba):.3f})')
axes[0, 0].plot([0, 1], [0, 1], color='navy', lw=1, linestyle='--')
axes[0, 0].fill_between(fpr, tpr, alpha=0.2, color='darkorange')
axes[0, 0].set_xlabel('False Positive Rate')
axes[0, 0].set_ylabel('True Positive Rate')
axes[0, 0].set_title('Curva ROC')
axes[0, 0].legend(loc='lower right')
axes[0, 0].grid(alpha=0.3)

# 2. Matriz de Confusão
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[0, 1],
            xticklabels=['Sem Vítimas', 'Com Vítimas'],
            yticklabels=['Sem Vítimas', 'Com Vítimas'])
axes[0, 1].set_title(f'Matriz de Confusão (threshold={THRESHOLD})')
axes[0, 1].set_xlabel('Predito')
axes[0, 1].set_ylabel('Real')

# 3. Métricas em barras
metricas_nomes = ['Accuracy', 'Precision', 'Recall', 'F1-Score']
metricas_valores = [accuracy_score(y_test, y_pred), 
                    precision_score(y_test, y_pred),
                    recall_score(y_test, y_pred), 
                    f1_score(y_test, y_pred)]
bars = axes[1, 0].bar(metricas_nomes, metricas_valores, 
                       color=['#2ecc71', '#3498db', '#e74c3c', '#9b59b6'])
axes[1, 0].set_ylim(0, 1)
axes[1, 0].set_ylabel('Score')
axes[1, 0].set_title('Métricas de Performance')
for bar, val in zip(bars, metricas_valores):
    axes[1, 0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, 
                    f'{val:.3f}', ha='center', va='bottom', fontweight='bold')

# 4. Feature Importance
importancias = pd.DataFrame({
    'feature': X_encoded.columns,
    'importance': rf.feature_importances_
}).sort_values('importance', ascending=False).head(15)

axes[1, 1].barh(importancias['feature'][::-1], importancias['importance'][::-1], 
                color='steelblue')
axes[1, 1].set_xlabel('Importância')
axes[1, 1].set_title('Top 15 Features Mais Importantes')
axes[1, 1].grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig(f'resultados_threshold_{THRESHOLD}.png', dpi=150, bbox_inches='tight')
plt.show()
print(f"✓ resultados_threshold_{THRESHOLD}.png salvo")

# =============================================================================
# 10. SALVAR MODELO
# =============================================================================

import joblib

joblib.dump(rf, 'modelo_binario_rf.pkl')
joblib.dump(list(X_encoded.columns), 'features_binario.pkl')

print("\\n✓ Modelos salvos:")
print("  - modelo_binario_rf.pkl")
print("  - features_binario.pkl")

print("\\n" + "=" * 70)
print(f"PIPELINE BINÁRIO CONCLUÍDO! (Threshold = {THRESHOLD})")
print("=" * 70)
