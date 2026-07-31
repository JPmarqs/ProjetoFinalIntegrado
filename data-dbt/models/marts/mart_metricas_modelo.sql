SELECT
    run_id,
    model_name,
    threshold,
    random_seed,
    train_rows,
    test_rows,
    rows_with_image,
    accuracy,
    balanced_accuracy,
    precision_score,
    recall_score,
    f1_score,
    roc_auc,
    true_negatives,
    false_positives,
    false_negatives,
    true_positives,
    artifact_s3_uri,
    created_at
FROM {{ source('ml_outputs', 'model_runs') }}

