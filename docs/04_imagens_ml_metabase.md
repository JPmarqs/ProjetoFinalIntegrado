# 4. Imagens, machine learning e Metabase

## Imagens por coordenada

`snowflake_mapbox_images_to_s3` consulta coordenadas únicas de
`INTERMEDIATE.INT_ACIDENTES`, chama a Static Images API e grava PNGs no S3.
Cada imagem gera uma linha em `ML.IMAGE_MANIFEST` com coordenada, estilo,
dimensão, ETag e geração do bucket.

O objeto `_pipeline/storage_generation.txt` recebe um UUID. Quando o laboratório
recria o bucket, surge outra geração e o manifesto antigo não é confundido com
objetos inexistentes.

`ML_IMAGE_BATCH_SIZE=5` é propositalmente conservador. Execute a DAG novamente
para obter o lote seguinte ou aumente o valor observando cota, tempo e custo.

## Treinamento

`train_accident_severity_model` usa `model/pipeline_integrado.py` e depois
executa os modelos `marts` do dbt.

O problema é uma classificação binária:

- alvo 1: acidente com vítimas feridas ou fatais;
- alvo 0: acidente sem vítimas;
- registros sem classificação não participam do treino.

São usadas variáveis categóricas do acidente, variáveis numéricas/temporais e,
quando disponíveis, cinco atributos simples extraídos da imagem: brilho médio,
desvio do brilho, proporção de verde, proporção aproximada de via e densidade de
bordas.

O pipeline aplica imputação, one-hot encoding, Random Forest com pesos de classe,
divisão estratificada de 75%/25%, semente 42 e threshold 0,37. O preprocessing e
o classificador são salvos juntos em `model.joblib`.

## Persistência

| Destino | Conteúdo |
|---|---|
| `s3://.../ml/artifacts/<run>/model.joblib` | Pipeline treinado |
| `s3://.../ml/artifacts/<run>/metrics.json` | Metadados e métricas |
| `ML.MODEL_RUNS` | Histórico dos treinos |
| `ML.MODEL_PREDICTIONS` | Previsões do conjunto de teste |
| `ML.FEATURE_IMPORTANCE` | Importância calculada pelo Random Forest |

## Resultado piloto

| Métrica | Valor |
|---|---:|
| Accuracy | 0,8653 |
| Balanced accuracy | 0,6246 |
| Precision | 0,8802 |
| Recall | 0,9730 |
| F1 | 0,9243 |
| ROC AUC | 0,7770 |

Matriz de confusão: TN 318, FP 833, FN 170 e TP 6.123, em 7.444 previsões.

Somente cinco acidentes do piloto possuíam imagem. Portanto, as métricas
demonstram a viabilidade do pipeline, não o benefício estatístico das imagens.

## Metabase

O Compose inicia Metabase e um PostgreSQL exclusivo para seus metadados. O
Metabase acessa somente os schemas `MART` e `ML` do Snowflake pela mesma chave
RSA montada em `/keys/snowflake_rsa_key.p8`.

O passo a passo e os SQLs das perguntas estão em
[08_metabase_dashboard.md](08_metabase_dashboard.md).
