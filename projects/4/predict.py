import os, sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel


model_path = sys.argv[1]
test_path = sys.argv[2]
ans_path = sys.argv[3]
model = PipelineModel.load(model_path)
schema = StructType([
    StructField("id", IntegerType()),
    StructField("overall", FloatType()),
    StructField("vote", StringType()), 
    StructField("verified", StringType()),
    StructField("reviewTime", StringType()),
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", LongType())
])

df = spark.read.json(test_path,schema=schema)
df = df.fillna({
    "reviewText": "missingreview",
})
predictions = model.transform(df)
predictions.select("id", "prediction") \
    .orderBy("id") \
    .coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(ans_path)

