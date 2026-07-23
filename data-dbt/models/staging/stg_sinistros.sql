{{ config(materialized='view') }}

SELECT *
FROM PROJETO_FINAL_IA.RAW.SINISTROS;
