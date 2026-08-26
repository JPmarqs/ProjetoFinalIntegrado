WITH acidentes AS (
    SELECT *
    FROM {{ ref('int_acidentes') }}
    WHERE target_com_vitimas IS NOT NULL
),

imagens_ranqueadas AS (
    SELECT
        coordinate_key,
        latitude AS image_latitude,
        longitude AS image_longitude,
        accident_count,
        storage_generation,
        s3_bucket,
        s3_object_key,
        content_type,
        etag,
        mapbox_style,
        mapbox_zoom,
        image_width,
        image_height,
        fetched_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY ROUND(latitude, 6), ROUND(longitude, 6)
            ORDER BY updated_at DESC NULLS LAST, fetched_at DESC NULLS LAST,
                     storage_generation DESC
        ) AS image_row
    FROM {{ source('ml_outputs', 'image_manifest') }}
),

imagens AS (
    SELECT *
    FROM imagens_ranqueadas
    WHERE image_row = 1
)

SELECT
    a.*,
    i.coordinate_key,
    i.image_latitude,
    i.image_longitude,
    i.accident_count,
    i.storage_generation,
    i.s3_bucket,
    i.s3_object_key,
    i.content_type,
    i.etag,
    i.mapbox_style,
    i.mapbox_zoom,
    i.image_width,
    i.image_height,
    i.fetched_at,
    i.updated_at,
    IFF(i.s3_object_key IS NOT NULL, 1, 0) AS possui_imagem
FROM acidentes a
LEFT JOIN imagens i
    ON ROUND(a.latitude, 6) = ROUND(i.image_latitude, 6)
   AND ROUND(a.longitude, 6) = ROUND(i.image_longitude, 6)
