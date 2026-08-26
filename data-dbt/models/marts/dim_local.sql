WITH locais AS (
    SELECT
        cd_bat,
        latitude,
        longitude,
        uf,
        municipio,
        rodovia,
        km,
        tipo_pista,
        estrutura_viaria,
        local_urbanizado,
        regiao,
        coordinate_key,
        s3_object_key,
        possui_imagem,
        updated_at
    FROM {{ ref('int_acidentes_imagens') }}
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
),

deduplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ROUND(latitude, 6), ROUND(longitude, 6)
            ORDER BY possui_imagem DESC, updated_at DESC NULLS LAST, cd_bat
        ) AS local_row
    FROM locais
)

SELECT
    MD5(CONCAT(ROUND(latitude, 6), '|', ROUND(longitude, 6))) AS local_key,
    latitude,
    longitude,
    coordinate_key,
    uf,
    municipio,
    rodovia,
    km,
    tipo_pista,
    estrutura_viaria,
    local_urbanizado,
    regiao,
    s3_object_key,
    possui_imagem
FROM deduplicados
WHERE local_row = 1
