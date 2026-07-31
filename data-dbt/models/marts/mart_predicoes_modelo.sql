WITH latest_run AS (
    SELECT run_id
    FROM {{ source('ml_outputs', 'model_runs') }}
    QUALIFY ROW_NUMBER() OVER (ORDER BY created_at DESC) = 1
),

predictions AS (
    SELECT p.*
    FROM {{ source('ml_outputs', 'model_predictions') }} p
    INNER JOIN latest_run r ON r.run_id = p.run_id
)

SELECT
    p.run_id,
    p.cd_bat,
    a.data_inversa,
    a.uf,
    a.regiao,
    a.municipio,
    a.rodovia,
    a.tipo_acidente,
    a.causa_acidente,
    a.latitude,
    a.longitude,
    p.actual_target,
    p.predicted_target,
    p.probability_com_vitimas,
    CASE
        WHEN p.actual_target = 1 AND p.predicted_target = 1 THEN 'Verdadeiro positivo'
        WHEN p.actual_target = 0 AND p.predicted_target = 0 THEN 'Verdadeiro negativo'
        WHEN p.actual_target = 0 AND p.predicted_target = 1 THEN 'Falso positivo'
        ELSE 'Falso negativo'
    END AS tipo_resultado
FROM predictions p
INNER JOIN {{ ref('int_acidentes') }} a ON a.cd_bat = p.cd_bat

