# 3. Ingestão, Snowflake e dbt

## Google Drive para S3

A DAG `google_drive_zip_csv_to_s3`:

1. baixa o arquivo público pelo ID do Google Drive;
2. confirma que o conteúdo é um ZIP válido;
3. localiza e extrai somente o CSV escolhido;
4. valida tamanho, texto e hash SHA-256;
5. envia o CSV a `s3://<bucket>/<S3_PREFIX>/`;
6. compara o tamanho local com o objeto remoto.

O arquivo intermediário fica em volume local ignorado pelo Git.

## S3 para RAW

A DAG `s3_to_snowflake_raw` valida o objeto e executa uma carga snapshot:

1. cria o file format compatível com a fonte (`;`, ISO-8859-1 e delimitador CR);
2. cria um stage externo temporário com as credenciais da sessão AWS;
3. carrega todas as colunas como texto em `RAW.SINISTROS_NEXT`;
4. valida se existem linhas;
5. publica o snapshot como `RAW.SINISTROS` por clone;
6. remove o stage temporário mesmo quando ocorre falha;
7. executa `dbt build --select staging`;
8. valida a view `STAGING.STG_SINISTROS`.

O snapshot validado produziu 249.251 linhas RAW; a última era apenas um byte de
quebra de linha. O staging remove exclusivamente esse registro vazio e mantém
249.250 registros válidos.

## Modelos dbt

```mermaid
flowchart LR
    R["RAW.SINISTROS<br/>texto + metadados"] --> S["STAGING.STG_SINISTROS<br/>tipagem"]
    S --> I["INTERMEDIATE.INT_ACIDENTES<br/>1 linha por CD_BAT"]
    I --> M1["MART_ACIDENTES_RESUMO"]
    I --> M2["MART_ACIDENTES_DIARIO"]
    O["ML outputs"] --> M3["MART_METRICAS_MODELO"]
    O --> M4["MART_PREDICOES_MODELO"]
    I --> M4
    O --> M5["MART_FEATURE_IMPORTANCE"]
```

`STAGING.STG_SINISTROS` converte IDs, quantidades, data, horário, quilômetro,
latitude e longitude. `INTERMEDIATE.INT_ACIDENTES` escolhe uma linha
representativa por acidente, priorizando causa principal e menor ordem do tipo
de acidente, e cria:

- `TARGET_COM_VITIMAS`;
- hora numérica;
- indicador de fim de semana;
- região do Brasil.

Os testes dbt verificam chaves obrigatórias, unicidade, coordenadas e valores
aceitos para o alvo. O resultado validado foi 29.775 acidentes únicos.

Para executar todos os modelos manualmente dentro de um container Airflow:

```bash
dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

