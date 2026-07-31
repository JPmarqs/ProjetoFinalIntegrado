# 8. Solução de problemas

## O script de chaves diz que elas já existem

É uma proteção, não uma falha. Execute sem `-Force` para apenas exibir a chave
pública existente. Use `-Force` somente se também for atualizar a chave no
Snowflake.

## `Requested role 'ACCOUNTADMIN' is not assigned`

A conta acadêmica não concede essa role. Use a role disponível e execute
`data-dbt/scripts/projeto_final.sql`. O roteiro `projeto_final_accountadmin.sql`
é exclusivo para uma conta administrada.

## Falha de autenticação RSA no Snowflake

Confirme:

- chave pública cadastrada no mesmo usuário de `SNOWFLAKE_USER`;
- `RSA_PUBLIC_KEY_FP` presente em `DESC USER <usuario>`;
- chave privada em `airflow-seminario/keys/snowflake_rsa_key.p8`;
- caminho interno `/opt/airflow/keys/snowflake_rsa_key.p8`;
- formato correto de `SNOWFLAKE_ACCOUNT`.

Depois recrie os containers.

## Credenciais AWS expiraram

Inicie outra sessão, substitua somente as três linhas em `.env.aws-lab` e rode:

```powershell
docker compose up -d --force-recreate
```

Recomece por `aws_lab_s3_bootstrap`. Apenas reiniciar uma task não atualiza o
ambiente já carregado no container.

## Um token ou chave já apareceu em um arquivo versionado

Remover o texto no commit seguinte não apaga o segredo do histórico. Revogue o
token no provedor, gere outro e mantenha o novo valor apenas em `.env` ou
`.env.aws-lab`. Antes de publicar o repositório, avalie também reescrever o
histórico com uma ferramenta própria, fazendo backup e coordenando a mudança com
o grupo.

## Bucket não pode ser criado

Verifique se o nome é globalmente único, se a região está correta e se o
laboratório ainda está ativo. Não use letras maiúsculas ou underscore no nome.

## Google Drive retornou HTML ou arquivo inválido

O arquivo precisa estar compartilhado para leitura pelo ID configurado. Confirme
que `SOURCE_FILENAME` termina em `.zip` e que `CSV_FILENAME` corresponde ao
arquivo interno.

## Snowflake reclama do delimitador ou cria uma linha extra

O arquivo usado possui `;`, encoding ISO-8859-1 e registros terminados por CR.
O file format usa `RECORD_DELIMITER='0x0D'`. O staging remove a linha final que
contém somente whitespace. Não troque por `AUTO` sem inspecionar outra fonte.

## Teste `not_null` falha em `ID_ENVOLVIDO`

Esse campo é legitimamente nulo em parte da fonte e não deve ter teste
`not_null`. A chave do acidente é `CD_BAT`.

## A DAG nova não aparece

Veja erros de importação e reinicie o processador:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose restart airflow-dag-processor
```

## Mapbox não grava imagens

Confirme token, cota, estilo no formato `usuario/style_id`, região AWS e
existência do objeto `_pipeline/storage_generation.txt`. Respostas que não sejam
PNG são rejeitadas intencionalmente.

## O treinamento falha por dependência ou tipo numérico

Reconstrua a imagem após alterar `requirements.txt`:

```powershell
docker compose build
docker compose up -d --force-recreate
```

Os dados categóricos e numéricos são normalizados antes do scikit-learn; confira
se o modelo dbt manteve os nomes esperados.

## Metabase não conecta com Snowflake

Use autenticação por arquivo RSA, sem senha, e o caminho
`/keys/snowflake_rsa_key.p8`. Confirme warehouse, database, role e acesso aos
schemas `MART,ML`.

## Metabase não inicia com `password authentication failed`

O volume PostgreSQL preserva a senha definida na primeira inicialização. Não
altere `METABASE_DB_PASSWORD` enquanto `metabase-db-volume` existir. Se a senha
for modificada por engano, restaure o valor anterior ou sincronize a senha do
usuário no PostgreSQL. Não remova o volume para corrigir o acesso, pois ele
contém usuários, perguntas e dashboards do Metabase.

## Dashboard não mostra o treino novo

Confirme que a task `build_analytics_marts` terminou, depois sincronize o schema
ou atualize a pergunta no Metabase. Os marts de previsões e importância sempre
selecionam a execução mais recente.
