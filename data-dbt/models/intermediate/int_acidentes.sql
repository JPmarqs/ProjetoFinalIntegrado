WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cd_bat
            ORDER BY
                CASE WHEN causa_principal = 'Sim' THEN 0 ELSE 1 END,
                COALESCE(ordem_tipo_acidente, 999999),
                COALESCE(id_envolvido, 999999999999)
        ) AS representative_row
    FROM {{ ref('stg_sinistros') }}
)

SELECT
    cd_bat,
    uf_acidente AS uf,
    rodovia,
    km,
    municipio,
    causa_acidente,
    tipo_acidente,
    fase_dia,
    sentido_via,
    cond_meteorologica,
    tipo_pista,
    estrutura_viaria,
    local_urbanizado,
    latitude,
    longitude,
    data_inversa,
    horario,
    DATE_PART('hour', horario) AS hora,
    dia_semana,
    classificacao_acidente,
    CASE
        WHEN classificacao_acidente = 'Sem Vítimas' THEN 0
        WHEN classificacao_acidente IN (
            'Com Vítimas Feridas',
            'Com Vítimas Fatais'
        ) THEN 1
    END AS target_com_vitimas,
    CASE
        WHEN LOWER(dia_semana) IN ('sábado', 'sabado', 'domingo') THEN 1
        ELSE 0
    END AS fim_de_semana,
    CASE
        WHEN uf_acidente IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
        WHEN uf_acidente IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
        WHEN uf_acidente IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN uf_acidente IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
        WHEN uf_acidente IN ('PR', 'RS', 'SC') THEN 'Sul'
        ELSE 'Desconhecida'
    END AS regiao,
    source_file,
    loaded_at
FROM ranked
WHERE representative_row = 1

