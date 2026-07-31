# Machine learning

## Implementação integrada

`pipeline_integrado.py` é a implementação usada pela DAG
`train_accident_severity_model`. Ela:

1. lê uma linha por acidente em `INTERMEDIATE.INT_ACIDENTES`;
2. associa imagens disponíveis no manifesto do bucket atual;
3. extrai atributos simples de brilho, vegetação, vias e bordas;
4. aplica imputação, one-hot encoding e Random Forest;
5. grava o pipeline completo e as métricas no S3;
6. persiste execução, previsões e importância das features no Snowflake.

O nome do database vem de `SNOWFLAKE_DATABASE`; não existe dependência fixa da
conta de treinamento.

## Arquivos exploratórios preservados

`radom_forest.py`, `radom_forest_sem_img.py` e `datatran2026.csv` registram a
experimentação inicial do grupo. Eles não são importados pelas DAGs e não devem
ser usados para executar o pipeline final. Foram preservados para rastreabilidade
acadêmica.

