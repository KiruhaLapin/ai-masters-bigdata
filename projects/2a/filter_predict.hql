CREATE EXTERNAL TABLE IF NOT EXISTS source_table_from_file (
    id STRING,
    if1 DOUBLE,
    if2 DOUBLE,
    if3 DOUBLE,
    if4 DOUBLE,
    if5 DOUBLE,
    if6 DOUBLE,
    if7 DOUBLE,
    if8 DOUBLE,
    if9 DOUBLE,
    if10 DOUBLE,
    if11 DOUBLE,
    if12 DOUBLE,
    if13 DOUBLE,
    if14 DOUBLE,
    cf1 DOUBLE,
    cf2 DOUBLE,
    cf3 DOUBLE,
    cf4 DOUBLE,
    cf5 DOUBLE,
    cf6 DOUBLE,
    cf7 DOUBLE,
    cf8 DOUBLE,
    cf9 DOUBLE,
    cf10 DOUBLE,
    cf11 DOUBLE,
    cf12 DOUBLE,
    cf13 DOUBLE,
    cf14 DOUBLE,
    cf15 DOUBLE,
    cf16 DOUBLE,
    cf17 DOUBLE,
    cf18 DOUBLE,
    cf19 DOUBLE,
    cf20 DOUBLE,
    cf21 DOUBLE,
    cf22 DOUBLE,
    cf23 DOUBLE,
    cf24 DOUBLE,
    cf25 DOUBLE,
    cf26 DOUBLE,
    cf27 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/datasets/criteo/testdir';

ADD FILE /projects/2a/predict.py;

INSERT INTO TABLE hw2_pred
SELECT
    TRANSFORM (id, if1, if2, if3, if4, if5, if6, if7, if8, if9, if10, if11, if12, if13, if14, cf1, cf2, cf3, cf4, cf5, cf6, cf7, cf8, cf9, cf10, cf11, cf12, cf13, cf14, cf15, cf16, cf17
    USING '/opt/conda/envs/dsenv/bin/python3 predict.py'
    AS (id STRING, prediction DOUBLE)
FROM
    source_table_from_file
WHERE
    if1 > 20 AND if1 < 40;
