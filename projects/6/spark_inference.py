import argparse
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-in', required=True, help='Path to input test data (CSV)')
    parser.add_argument('--pred-out', required=True, help='Path to output predictions (Parquet)')
    parser.add_argument('--sklearn-model-in', required=True, help='Path to trained sklearn model (joblib)')
    return parser.parse_args()

def main():
    args = parse_args()

    # Создание Spark-сессии
    spark = SparkSession.builder.appName("SparkInference").getOrCreate()

    # Загрузка данных из CSV
    test_df = spark.read.csv(args.test_in, header=True, inferSchema=True)

    # Загрузка модели
    model = joblib.load(args.sklearn_model_in)

    # Определение признаков
    feature_columns = [col for col in test_df.columns if col != 'id']

    # UDF для предсказания
    @pandas_udf(DoubleType())
    def predict_udf(*cols: pd.Series) -> pd.Series:
        features = pd.concat(cols, axis=1)
        return pd.Series(model.predict(features))

    # Применяем модель
    df_with_pred = test_df.withColumn(
        'prediction',
        predict_udf(*[test_df[col] for col in feature_columns])
    )

    # Сохраняем результат
    df_with_pred.select('id', 'prediction') \
        .write.mode('overwrite').parquet(args.pred_out)

    spark.stop()

if __name__ == '__main__':
    main()



