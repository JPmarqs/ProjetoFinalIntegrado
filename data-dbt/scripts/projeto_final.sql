-- =============================================================
-- PARTE 1 - Bootstrap na conta de treinamento Snowflake
--
-- Ambiente confirmado:
--   usuario:   BEETLE
--   role:      TRAINING_ROLE
--   warehouse: BEETLE_WH
--   database:  BEETLE_DB
--
-- A chave RSA do usuario BEETLE deve ter sido cadastrada antes.
-- Este script nao cria usuario, role, warehouse ou database.
-- =============================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE BEETLE_WH;
USE DATABASE BEETLE_DB;

CREATE SCHEMA IF NOT EXISTS BEETLE_DB.RAW
    COMMENT = 'Dados brutos carregados do S3';

CREATE SCHEMA IF NOT EXISTS BEETLE_DB.STAGING
    COMMENT = 'Padronizacao inicial realizada pelo dbt';

CREATE SCHEMA IF NOT EXISTS BEETLE_DB.INTERMEDIATE
    COMMENT = 'Transformacoes intermediarias e enriquecimentos';

CREATE SCHEMA IF NOT EXISTS BEETLE_DB.MART
    COMMENT = 'Tabelas analiticas consumidas pelo Metabase';

CREATE SCHEMA IF NOT EXISTS BEETLE_DB.ML
    COMMENT = 'Features, predicoes e metricas de machine learning';

-- Valores que serao usados no arquivo airflow-seminario/.env.
SELECT
    CURRENT_USER() AS SNOWFLAKE_USER,
    CURRENT_ROLE() AS SNOWFLAKE_ROLE,
    CURRENT_WAREHOUSE() AS SNOWFLAKE_WAREHOUSE,
    CURRENT_DATABASE() AS SNOWFLAKE_DATABASE,
    CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME()
        AS SNOWFLAKE_ACCOUNT;

SHOW SCHEMAS IN DATABASE BEETLE_DB;
