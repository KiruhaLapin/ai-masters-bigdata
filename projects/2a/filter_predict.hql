ADD FILE projects/2a/predict.py;

INSERT INTO TABLE hw2_pred
SELECT
    TRANSFORM (id, if1, if2, if3, if4, if5, if6, if7, if8, if9, if10, if11, if12, if13, if14, cf1, cf2, cf3, cf4, cf5, cf6, cf7, cf8, cf9, cf10, cf11, cf12, cf13, cf14, cf15, cf16, cf17, cf18, cf19, cf20, cf21, cf22, cf23, cf24, cf25, cf26, cf27)
    USING '/opt/conda/envs/dsenv/bin/python3 predict.py'
    AS (id STRING, prediction DOUBLE)
FROM
    source_table
WHERE
    if1 > 20 AND if1 < 40;
