WITH latest_run AS (
    SELECT run_id
    FROM {{ source('ml_outputs', 'model_runs') }}
    QUALIFY ROW_NUMBER() OVER (ORDER BY created_at DESC) = 1
)

SELECT
    f.run_id,
    f.feature_name,
    f.importance,
    f.feature_rank,
    f.created_at
FROM {{ source('ml_outputs', 'feature_importance') }} f
INNER JOIN latest_run r ON r.run_id = f.run_id

