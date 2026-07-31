# 1. Snowflake, dbt e autenticação

## Pré-requisitos

- Docker Desktop com pelo menos 4 GB disponíveis;
- PowerShell;
- conta Snowflake com warehouse e permissão para criar schemas;
- conta Mapbox com token da Static Images API;
- acesso ao laboratório AWS quando for executar o pipeline completo.

## Gerar o par de chaves Snowflake

Na raiz do projeto:

```powershell
powershell -ExecutionPolicy Bypass -File .\airflow-seminario\scripts\generate_snowflake_key_pair.ps1
```

O script cria:

- `airflow-seminario/keys/snowflake_rsa_key.p8`: chave privada, ignorada pelo Git;
- `airflow-seminario/keys/snowflake_rsa_key.pub`: chave pública.

Se os arquivos já existirem, o script não os substitui. Use `-Force` somente
para uma rotação planejada e cadastre a nova chave pública no Snowflake.

## Preparar o Snowflake

Há dois roteiros:

- conta acadêmica já provisionada: execute
  `data-dbt/scripts/projeto_final.sql` com a role disponível;
- conta administrada: substitua o placeholder da chave e execute
  `data-dbt/scripts/projeto_final_accountadmin.sql` como `ACCOUNTADMIN`.

Ao final devem existir os schemas:

| Schema | Finalidade |
|---|---|
| `RAW` | Snapshot textual do CSV |
| `STAGING` | Tipagem e padronização |
| `INTERMEDIATE` | Uma linha por acidente e features de negócio |
| `ML` | Manifesto, execuções, previsões e importâncias |
| `MART` | Tabelas prontas para o Metabase |

O script administrado também cria warehouse, role e usuário técnico. O script
acadêmico não tenta assumir `ACCOUNTADMIN`.

## Configurar `.env`

```powershell
Copy-Item .\airflow-seminario\.env.example .\airflow-seminario\.env
```

Preencha os valores locais. O identificador `SNOWFLAKE_ACCOUNT` normalmente usa
o formato `ORGANIZACAO-CONTA`, retornado pelo `SELECT` dos scripts SQL.

Não altere o caminho interno da chave:

```dotenv
SNOWFLAKE_PRIVATE_KEY_PATH=/opt/airflow/keys/snowflake_rsa_key.p8
DBT_PROFILES_DIR=/opt/airflow/dbt
```

## Construir e iniciar os containers

```powershell
Set-Location .\airflow-seminario
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Airflow: `http://localhost:8080`. Metabase: `http://localhost:3000`.

## Validar Airflow e dbt

Acione `snowflake_dbt_diagnostic`. Ela executa:

1. uma consulta do contexto atual usando `snowflake_default`;
2. `dbt debug` com o mesmo usuário e a mesma chave.

A DAG é apenas diagnóstica e não altera dados. As credenciais não ficam em
`profiles.yml`: o dbt lê as variáveis do ambiente.

