-- =========================================================
-- Projeto Final IA
-- Script de Inicialização do Ambiente Snowflake
-- Responsável: Rodolfo
-- =========================================================

-- Criação do Database
CREATE DATABASE IF NOT EXISTS PROJETO_FINAL_IA;

USE DATABASE PROJETO_FINAL_IA;

-- Criação dos Schemas
CREATE SCHEMA IF NOT EXISTS RAW;

CREATE SCHEMA IF NOT EXISTS STAGING;

CREATE SCHEMA IF NOT EXISTS MART;

CREATE SCHEMA IF NOT EXISTS ML;

-- Validação
SELECT CURRENT_DATABASE();

SHOW SCHEMAS;