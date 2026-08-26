WITH acidentes_deduplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cd_bat
            ORDER BY possui_imagem DESC, updated_at DESC NULLS LAST,
                     s3_object_key NULLS LAST
        ) AS accident_row
    FROM {{ ref('int_acidentes_imagens') }}
    WHERE target_com_vitimas IS NOT NULL
)

SELECT
    a.cd_bat,
    d.local_key,
    a.data_inversa,
    a.horario,
    a.hora,
    a.dia_semana,
    a.uf,
    a.rodovia,
    a.km,
    a.municipio,
    a.latitude,
    a.longitude,
    a.causa_acidente,
    a.tipo_acidente,
    a.classificacao_acidente,
    a.fase_dia,
    a.sentido_via,
    a.cond_meteorologica,
    a.tipo_pista,
    a.estrutura_viaria,
    a.local_urbanizado,
    a.target_com_vitimas,
    a.fim_de_semana,
    a.regiao,
    a.possui_imagem,
    a.coordinate_key,
    a.storage_generation,
    a.s3_bucket,
    a.s3_object_key
FROM acidentes_deduplicados a
LEFT JOIN {{ ref('dim_local') }} d
    ON ROUND(a.latitude, 6) = ROUND(d.latitude, 6)
   AND ROUND(a.longitude, 6) = ROUND(d.longitude, 6)
WHERE a.accident_row = 1
