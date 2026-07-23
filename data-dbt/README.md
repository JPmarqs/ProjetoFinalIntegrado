# Projeto Final Integrado - Data/dbt

Este repositório contém a parte inicial da etapa de dados do projeto, com foco na preparação do ambiente no Snowflake e na estrutura inicial do dbt.

## Estrutura atual

- `scripts/`  
  Contém os scripts e instruções usados na preparação do ambiente e na carga inicial dos mocks no Snowflake.

- `models/staging/`  
  Contém o modelo `stg_sinistros.sql`, responsável pela camada inicial de staging no dbt.

- `dbt_project.yml`  
  Arquivo de configuração principal do projeto dbt.

- `logs/`  
  Contém os logs gerados durante a execução e validação do dbt.

## O que esta parte do projeto representa

Esta entrega corresponde à etapa de configuração da base analítica e da estrutura inicial de transformação de dados, incluindo:

- organização inicial do ambiente Snowflake;
- preparação para carga de dados mock;
- configuração do projeto dbt;
- criação do primeiro modelo de staging;
- registro das evidências de execução.

## Objetivo

Deixar a base pronta para a evolução do pipeline de dados, com novas transformações, validações e integração com as próximas etapas do projeto.
