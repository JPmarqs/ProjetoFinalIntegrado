# Ambiente Airflow e Metabase

Esta pasta contém o ambiente Docker Compose do projeto, as DAGs e os arquivos de
configuração local.

## Arquivos principais

- `docker-compose.yaml`: Airflow 3, PostgreSQL, Redis e Metabase;
- `Dockerfile` e `requirements.txt`: imagem Airflow com providers, dbt e ML;
- `dags/`: fluxos operacionais e diagnóstico;
- `dags/parquet_utils.py`: conversão incremental e validação CSV → Parquet;
- `scripts/generate_snowflake_key_pair.ps1`: geração segura do par RSA;
- `.env.example`: configuração permanente sem segredos;
- `.env.aws-lab.example`: modelo para as três credenciais AWS temporárias.

## Iniciar

No PowerShell:

```powershell
Copy-Item .env.example .env
Copy-Item .env.aws-lab.example .env.aws-lab
# Preencha os dois arquivos locais e gere/cadastre a chave Snowflake.
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Interfaces locais:

- Airflow: `http://localhost:8080`
- Metabase: `http://localhost:3000`

Consulte a [execução ponta a ponta](../docs/05_execucao_ponta_a_ponta.md) antes
de acionar as DAGs. Em uma nova sessão do laboratório AWS, atualize somente
`.env.aws-lab`, recrie os containers e execute `aws_lab_s3_bootstrap`.
