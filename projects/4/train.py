import os,sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from model import pipeline

train_path = sys.argv[1]
model_path = sys.argv[2]

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

df = spark.read.json(train_path, schema=schema)
df = df.fillna({
    "reviewText": "missingreview",
})

pipeline_model = pipeline.fit(df)
pipeline_model.write().overwrite().save(model_path)
