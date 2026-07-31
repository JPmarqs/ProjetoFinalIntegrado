# 9. Dashboard Metabase

## Conexão Snowflake

Em `http://localhost:3000`, adicione um banco Snowflake:

| Campo | Configuração |
|---|---|
| Account | locator ou identificador solicitado pela interface |
| Username | mesmo usuário técnico do Airflow |
| Password | vazio |
| RSA private key | arquivo local `/keys/snowflake_rsa_key.p8` |
| Warehouse | valor de `SNOWFLAKE_WAREHOUSE` |
| Database | valor de `SNOWFLAKE_DATABASE` |
| Schemas | `MART,ML` |
| Role | valor de `SNOWFLAKE_ROLE` |

Os exemplos abaixo usam `MART.<tabela>` porque o database já está definido na
conexão.

## KPIs do último treino

Crie uma pergunta para cada métrica, trocando o campo selecionado:

```sql
SELECT ROUND(F1_SCORE, 2) AS VALOR
FROM MART.MART_METRICAS_MODELO
QUALIFY ROW_NUMBER() OVER (ORDER BY CREATED_AT DESC) = 1;
```

Campos usados nos quatro cartões: `F1_SCORE`, `BALANCED_ACCURACY`,
`RECALL_SCORE` e `ROC_AUC`.

## Total de acidentes

```sql
SELECT COUNT(*) AS TOTAL_ACIDENTES
FROM MART.MART_ACIDENTES_RESUMO;
```

## Percentual com vítimas

```sql
SELECT ROUND(
    100.0 * COUNT_IF(TARGET_COM_VITIMAS = 1)
    / NULLIF(COUNT_IF(TARGET_COM_VITIMAS IS NOT NULL), 0),
    2
) AS PERCENTUAL_COM_VITIMAS
FROM MART.MART_ACIDENTES_RESUMO;
```

## Matriz de confusão

```sql
SELECT
    CASE ACTUAL_TARGET
        WHEN 0 THEN 'Real: Sem vítimas'
        ELSE 'Real: Com vítimas'
    END AS CLASSE_REAL,
    COUNT_IF(PREDICTED_TARGET = 0) AS "Previsto: Sem vítimas",
    COUNT_IF(PREDICTED_TARGET = 1) AS "Previsto: Com vítimas"
FROM MART.MART_PREDICOES_MODELO
GROUP BY ACTUAL_TARGET
ORDER BY ACTUAL_TARGET;
```

Use visualização de tabela e formatação condicional verde na diagonal e vermelha
fora dela.

## Evolução diária

```sql
SELECT
    DATA_INVERSA,
    SUM(TOTAL_ACIDENTES) AS TOTAL_ACIDENTES,
    SUM(ACIDENTES_COM_VITIMAS) AS COM_VITIMAS,
    SUM(ACIDENTES_SEM_VITIMAS) AS SEM_VITIMAS
FROM MART.MART_ACIDENTES_DIARIO
GROUP BY DATA_INVERSA
ORDER BY DATA_INVERSA;
```

Use gráfico de linhas com `DATA_INVERSA` no eixo X.

## Mapa

```sql
SELECT
    CD_BAT,
    LATITUDE,
    LONGITUDE,
    UF,
    MUNICIPIO,
    CLASSIFICACAO_ACIDENTE
FROM MART.MART_ACIDENTES_RESUMO
WHERE LATITUDE IS NOT NULL
  AND LONGITUDE IS NOT NULL;
```

Escolha mapa de pontos e configure latitude e longitude.

## Top 10 UFs

```sql
SELECT
    UF,
    COUNT_IF(TARGET_COM_VITIMAS = 1) AS COM_VITIMAS,
    COUNT_IF(TARGET_COM_VITIMAS = 0) AS SEM_VITIMAS,
    COUNT(*) AS TOTAL
FROM MART.MART_ACIDENTES_RESUMO
GROUP BY UF
ORDER BY TOTAL DESC
LIMIT 10;
```

Use barras empilhadas com as duas séries de vítimas.

## Classificação dos acidentes

```sql
SELECT
    COALESCE(CLASSIFICACAO_ACIDENTE, 'Não informado') AS CLASSIFICACAO,
    COUNT(*) AS TOTAL
FROM MART.MART_ACIDENTES_RESUMO
GROUP BY CLASSIFICACAO_ACIDENTE
ORDER BY TOTAL DESC;
```

Use gráfico de rosca e limite percentuais a duas casas decimais.

## Top 15 features

```sql
SELECT
    REGEXP_REPLACE(
        FEATURE_NAME,
        '^(categorical|numeric)__',
        ''
    ) AS FEATURE,
    IMPORTANCE
FROM MART.MART_FEATURE_IMPORTANCE
WHERE FEATURE_RANK <= 15
ORDER BY FEATURE_RANK;
```

Use barras horizontais para manter os nomes legíveis.

## Filtros opcionais

Nas perguntas SQL sobre acidentes, adicione:

```sql
WHERE 1 = 1
[[AND {{uf}}]]
[[AND {{periodo}}]]
[[AND {{classificacao}}]]
```

Se a consulta já possuir `WHERE`, mantenha-o e acrescente apenas os blocos
`[[AND ...]]` antes do `GROUP BY`.

Configure cada variável como **Field Filter** e associe aos campos `UF`,
`DATA_INVERSA` e `CLASSIFICACAO_ACIDENTE` da tabela usada pela pergunta. Não
conecte esses filtros aos KPIs globais do treino, à matriz de confusão ou à
importância das features.
