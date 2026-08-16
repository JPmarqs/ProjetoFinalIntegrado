# 6. Arquitetura e fluxos

## Visão de componentes

```mermaid
flowchart TB
    subgraph ORQ["Orquestração local - Docker Compose"]
        AF["Apache Airflow"]
        MB["Metabase"]
        PG1["PostgreSQL Airflow"]
        PG2["PostgreSQL Metabase"]
        RD["Redis"]
        AF --- PG1
        AF --- RD
        MB --- PG2
    end

    GD["Google Drive<br/>dataset PRF"] --> AF
    AF --> S3["Amazon S3 efêmero<br/>raw, mapbox e artefatos"]
    AF --> SF["Snowflake"]
    AF --> MP["Mapbox Static Images API"]
    MP --> AF
    SF --> DBT["dbt executado pelo Airflow"]
    DBT --> SF
    SF --> MB
```

## Fluxo lógico dos dados

```mermaid
flowchart LR
    CSV["CSV PRF<br/>grão: envolvido"] --> PQ["Parquet no S3<br/>37 colunas texto"]
    PQ --> RAW["RAW<br/>contrato textual da fonte"]
    RAW --> STG["STAGING<br/>tipos e limpeza mínima"]
    STG --> INT["INTERMEDIATE<br/>grão: acidente"]
    INT --> IMG["Imagens por coordenada"]
    INT --> TRAIN["Treino/teste Random Forest"]
    IMG --> TRAIN
    TRAIN --> OUT["ML<br/>runs, predictions, importance"]
    INT --> MART["MART<br/>análise de acidentes"]
    OUT --> MART
    MART --> DASH["Dashboard Metabase"]
```

O ponto crítico de modelagem é a troca de grão. O CSV possui várias pessoas ou
veículos por acidente; o modelo usa uma única linha por `CD_BAT`. Isso evita que
o mesmo acidente apareça simultaneamente em treino e teste, reduzindo vazamento
de informação.

## Fronteira efêmera

```mermaid
flowchart LR
    LAB["Sessão AWS temporária"] --> CRED[".env.aws-lab<br/>3 credenciais"]
    CRED --> AF["Airflow"]
    AF --> BKT["Bucket S3 recriado"]
    BKT --> GEN["storage_generation UUID"]
    GEN --> MAN["Manifesto Snowflake da geração atual"]
```

O UUID impede que metadados de um bucket anterior sejam interpretados como
imagens disponíveis no bucket novo.

## Escolhas de responsabilidade

- Airflow coordena tarefas e dependências; não é usado como motor analítico.
- Snowflake executa a carga e armazena tabelas persistentes.
- dbt concentra transformações SQL e testes de qualidade.
- Python/scikit-learn concentra preprocessing e treinamento.
- S3 armazena objetos grandes; Snowflake armazena referências e resultados.
- Metabase consulta apenas tabelas de consumo, sem duplicar regras de negócio.
