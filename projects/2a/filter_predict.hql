CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS source_table_from_file (
    id INT,
    if1 INT,
    if2 INT,
    if3 INT,
    if4 INT,
    if5 INT,
    if6 INT,
    if7 INT,
    if8 INT,
    if9 INT,
    if10 INT,
    if11 INT,
    if12 INT,
    if13 INT,
    cf1 STRING,
    cf2 STRING,
    cf3 STRING,
    cf4 STRING,
    cf5 STRING,
    cf6 STRING,
    cf7 STRING,
    cf8 STRING,
    cf9 STRING,
    cf10 STRING,
    cf11 STRING,
    cf12 STRING,
    cf13 STRING,
    cf14 STRING,
    cf15 STRING,
    cf16 STRING,
    cf17 STRING,
    cf18 STRING,
    cf19 STRING,
    cf20 STRING,
    cf21 STRING,
    cf22 STRING,
    cf23 STRING,
    cf24 STRING,
    cf25 STRING,
    cf26 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '/datasets/criteo/testdir';

SELECT * FROM source_table_from_file LIMIT 10;


ADD FILE projects/2a/predict.py;

INSERT INTO TABLE hw2_pred
SELECT
    TRANSFORM (id, if1, if2, if3, if4, if5, if6, if7, if8, if9, if10, if11, if12, if13, cf1, cf2, cf3, cf4, cf5, cf6, cf7, cf8, cf9, cf10, cf11, cf12, cf13, cf14, cf15, cf16, cf17, cf18, cf19, cf20, cf21, cf22, cf23, cf24, cf25, cf26)
    USING '/opt/conda/envs/dsenv/bin/python3 predict.py'
    AS (id, prediction)
FROM
    source_table_from_file
WHERE
    if1 > 20 AND if1 < 40;
