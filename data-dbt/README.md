# Projeto dbt

O dbt transforma a carga textual do Snowflake em modelos tipados, testados e
adequados ao machine learning e ao Metabase.

## Linhagem

```mermaid
flowchart LR
    R["RAW.SINISTROS"] --> S["STAGING.STG_SINISTROS"]
    S --> I["INTERMEDIATE.INT_ACIDENTES"]
    I --> A["MART.MART_ACIDENTES_RESUMO"]
    I --> D["MART.MART_ACIDENTES_DIARIO"]
    ML["ML.MODEL_RUNS"] --> M["MART.MART_METRICAS_MODELO"]
    ML2["ML.MODEL_PREDICTIONS"] --> P["MART.MART_PREDICOES_MODELO"]
    I --> P
    ML3["ML.FEATURE_IMPORTANCE"] --> F["MART.MART_FEATURE_IMPORTANCE"]
```

## Camadas

- `staging`: padroniza nomes, converte datas/números e remove somente a linha
  fantasma vazia do arquivo de origem;
- `intermediate`: consolida uma linha representativa por `CD_BAT` e cria o alvo
  binário `TARGET_COM_VITIMAS`;
- `marts`: publica tabelas consumidas pelo dashboard.

O macro `generate_schema_name` evita que o dbt concatene o schema do perfil ao
schema configurado no modelo.

## Comandos dentro do container

```bash
dbt debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

As credenciais são obtidas exclusivamente por variáveis de ambiente e pela
chave RSA montada no container. Veja [docs/03_s3_snowflake_dbt.md](../docs/03_s3_snowflake_dbt.md).
