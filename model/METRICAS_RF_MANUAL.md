# Métricas — Random Forest Hard-Code (`rf_manual.py`)

> **Execução:** 2026-08-12 | **Dataset:** datatran2026.csv (PRF) | **Threshold:** 0,37

---

## Dataset

| Item | Valor |
|------|-------|
| Registros originais | 4.565 linhas × 33 colunas |
| Registros válidos | 4.564 |
| Features após encoding | 110 colunas (108 categóricas + 2 de imagem) |
| Imagens encontradas | 4.563 / 4.564 |

### Distribuição do target binário

| Classe | Quantidade | Proporção |
|--------|-----------|-----------|
| 0 — Sem Vítimas | 807 | 17,68% |
| 1 — Com Vítimas | 3.757 | 82,32% |

### Divisão dos dados

| Conjunto | Amostras |
|----------|----------|
| Treino (antes do SMOTE) | 3.424 |
| Teste | 1.140 |

### SMOTE

| Momento | Classe 0 | Classe 1 |
|---------|----------|----------|
| Antes | 606 | 2.818 |
| Depois | 2.818 | 2.818 |

---

## Métricas de Performance (Threshold = 0,37)

| Métrica | Valor |
|---------|-------|
| **Accuracy** | 0,8237 |
| **Balanced Accuracy** | 0,5000 |
| **Precision** | 0,8237 |
| **Recall** | 1,0000 |
| **F1-Score** | 0,9033 |
| **AUC-ROC** | 0,7108 |

---

## Cross-Validation (5 folds)

| Métrica | Média | IC 95% |
|---------|-------|--------|
| **F1-Score** | 0,8562 | ± 0,0085 |

---

## Classification Report

```
                      precision     recall   f1-score    support

         Sem Vítimas     0.0000     0.0000     0.0000        201
         Com Vítimas     0.8237     1.0000     0.9033        939

            accuracy                           0.8237       1140
           macro avg     0.4118     0.5000     0.4517       1140
        weighted avg     0.6785     0.8237     0.7440       1140
```

---

## Matriz de Confusão

```
                   Pred: Sem Vítimas  Pred: Com Vítimas
Real: Sem Vítimas                  0                201
Real: Com Vítimas                  0                939
```

| Positivo | Valor |
|----------|-------|
| VP (Verdadeiros Positivos) | 939 |
| VN (Verdadeiros Negativos) | 0 |
| FP (Falsos Positivos) | 201 |
| FN (Falsos Negativos) | 0 |

---

## Análise de Impacto Prático

| Indicador | Valor |
|-----------|-------|
| Total de acidentes no teste | 1.140 |
| ✓ Vítimas CORRETAMENTE atendidas | 939 (100,0%) |
| ✓ Não-despachos corretos | 0 |
| ⚠ Despachos DESNECESSÁRIOS | 201 (100,0%) |
| ✗ Vítimas NÃO atendidas | 0 (0,0%) |

---

## Comparação de Thresholds

| Threshold | Accuracy | Precision | Recall | F1 | FN | FP |
|-----------|----------|-----------|--------|----|----|-----|
| 0,30 | 0,8237 | 0,8237 | 1,0000 | 0,9033 | 0 | 201 |
| **0,37** | **0,8237** | **0,8237** | **1,0000** | **0,9033** | **0** | **201** |
| 0,40 | 0,8237 | 0,8237 | 1,0000 | 0,9033 | 0 | 201 |
| 0,50 | 0,8307 | 0,8295 | 1,0000 | 0,9068 | 0 | 193 |
| 0,60 | 0,8412 | 0,8687 | 0,9510 | 0,9080 | 46 | 135 |
| 0,70 | 0,2649 | 0,9244 | 0,1171 | 0,2079 | 829 | 9 |

---

## Parâmetros do Modelo

| Hiperparâmetro | Valor |
|----------------|-------|
| `n_estimators` | 300 |
| `max_depth` | 30 |
| `min_samples_split` | 5 |
| `min_samples_leaf` | 2 |
| `max_features` | 'sqrt' (10 features/split) |
| `random_state` | 42 |
| Tempo de treino | 159,15 s |

---

*Arquivo gerado a partir da execução de `python rf_manual.py`*
