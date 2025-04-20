import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, length, to_date, year, month, dayofmonth


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Feature Engineering for Amazon Reviews")
    parser.add_argument('--path-in', required=True, help='HDFS путь к входному JSON-файлу')
    parser.add_argument('--path-out', required=True, help='HDFS путь для сохранения выходных CSV-файлов')
    return parser.parse_args()


def main(path_in: str, path_out: str):
    spark = SparkSession.builder.appName("spark_feature_eng").getOrCreate()
    df = spark.read.json(path_in)
    df = df.withColumn('vote', col('vote').cast('int'))
    df = df.fillna({'vote': 0})
    df = df.withColumn('verified', when(col('verified') == True, 1).otherwise(0))
    df = df.withColumn('review_date', to_date(col('reviewTime'), 'MM dd, yyyy'))
    df = df.withColumn('review_year', year(col('review_date')))
    df = df.withColumn('review_month', month(col('review_date')))
    df = df.withColumn('review_day', dayofmonth(col('review_date')))
    df = df.withColumn('review_len', length(col('reviewText')))
    df = df.withColumn('summary_len', length(col('summary')))
    base_cols = ['id', 'vote', 'verified', 'review_year', 'review_month', 'review_day', 'review_len', 'summary_len']
    if 'label' in df.columns:
        cols = ['label'] + base_cols
    else:
        cols = base_cols
    df.select(*cols) \
      .coalesce(1) \
      .write \
      .option('header', True) \
      .csv(path_out, mode='overwrite')

    spark.stop()


if __name__ == '__main__':
    args = parse_args()
    main(args.path_in, args.path_out)
