from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF,
    StringIndexer, VectorAssembler
)
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import concat_ws, coalesce, lit

tokenizer_review = Tokenizer(inputCol="reviewText", outputCol="review_words")
remover_review = StopWordsRemover(inputCol="review_words", outputCol="filtered_review_words")
tf_review = HashingTF(inputCol="filtered_review_words", outputCol="raw_review_features", numFeatures=100)
idf_review = IDF(inputCol="raw_review_features", outputCol="review_features")


assembler = VectorAssembler(
    inputCols=["review_features", "verified"],
    outputCol="features"
)

regressor = LinearRegression(
    labelCol="overall",
    featuresCol="features",
    regParam=0.1,  # Регуляризация для предотвращения переобучения
)

pipeline = Pipeline(stages=[
    tokenizer_review, remover_review, tf_review, idf_review,
    assembler,
    regressor
])

