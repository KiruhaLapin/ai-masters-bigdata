CREATE TABLE IF NOT EXISTS KiruhaLapin_hw2_pred (
    id STRING,
    prediction DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:/user/KiruhaLapin_hw2_pred';

