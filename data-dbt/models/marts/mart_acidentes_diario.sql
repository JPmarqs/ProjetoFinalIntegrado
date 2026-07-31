SELECT
    data_inversa,
    uf,
    regiao,
    classificacao_acidente,
    COUNT(*) AS total_acidentes,
    COUNT_IF(target_com_vitimas = 1) AS acidentes_com_vitimas,
    COUNT_IF(target_com_vitimas = 0) AS acidentes_sem_vitimas
FROM {{ ref('int_acidentes') }}
GROUP BY
    data_inversa,
    uf,
    regiao,
    classificacao_acidente

