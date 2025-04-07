from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF,
    StringIndexer, VectorAssembler
)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import concat_ws, coalesce, lit


tokenizer_review = Tokenizer(inputCol="reviewText", outputCol="review_words")
remover_review = StopWordsRemover(inputCol="review_words", outputCol="filtered_review_words")
tf_review = HashingTF(inputCol="filtered_review_words", outputCol="raw_review_features", numFeatures=5000)
idf_review = IDF(inputCol="raw_review_features", outputCol="review_features")

verified_indexer = StringIndexer(inputCol="verified", outputCol="verified_idx")
asin_indexer = StringIndexer(inputCol="asin", outputCol="asin_idx")

assembler = VectorAssembler(
    inputCols=["review_features", "verified_idx", "asin_idx"],
    outputCol="features"
)

regressor = RandomForestRegressor(labelCol="overall", featuresCol="features")

pipeline = Pipeline(stages=[
    tokenizer_review, remover_review, tf_review, idf_review,
    verified_indexer, asin_indexer,
    assembler,
    regressor
])
