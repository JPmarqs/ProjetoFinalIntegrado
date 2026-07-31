# 2. Laboratório AWS e S3 efêmero

O laboratório fornece uma sessão de curta duração. Quando ela expira, as
credenciais deixam de funcionar e o bucket pode ser apagado. O Snowflake e o
banco interno do Metabase permanecem fora desse ciclo.

## Separação das configurações

- `.env`: região, bucket, prefixos e demais configurações permanentes;
- `.env.aws-lab`: somente `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` e
  `AWS_SESSION_TOKEN` temporários.

Prepare o arquivo temporário:

```powershell
Copy-Item .\airflow-seminario\.env.aws-lab.example `
  .\airflow-seminario\.env.aws-lab
```

Cole as três credenciais da sessão atual. Nunca as inclua em documentação,
prints, commits ou mensagens.

## Início de cada nova sessão

Depois de atualizar `.env.aws-lab`, recrie os containers para que todos recebam
o novo ambiente:

```powershell
Set-Location .\airflow-seminario
docker compose up -d --force-recreate
docker compose ps
```

Acione `aws_lab_s3_bootstrap`. A DAG:

1. valida a identidade com AWS STS;
2. cria o bucket se necessário;
3. reafirma o bloqueio de acesso público quando permitido;
4. valida o prefixo de ingestão;
5. nunca exclui buckets ou objetos.

Use um nome de bucket globalmente único. A região configurada precisa ser
compatível com a região do laboratório.

## Decisão acadêmica e alternativa de produção

Na carga Snowflake, o projeto cria um external stage temporário com o token da
sessão e o remove após o `COPY INTO`. Isso atende ao laboratório efêmero. Em
produção, a abordagem indicada é uma Snowflake Storage Integration associada a
uma IAM role persistente, sem chaves na sessão do Airflow.

