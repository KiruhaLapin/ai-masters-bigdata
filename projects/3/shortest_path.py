import os, sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

conf = SparkConf()

spark = SparkSession.builder \
    .appName("Pagerank") \
    .getOrCreate()


start = sys.argv[1]
finish = sys.argv[2]
path_to_df = sys.argv[3]
path_to_ans = sys.argv[4]

def find_shortest_path(spark, edges_df, start_user, target_user, max_iter=100):
    followers_graph = edges_df.groupBy("user_id")\
                             .agg(F.collect_list("follower_id").alias("followers"))
    distances = spark.createDataFrame(
        [(start_user, 0, [str(start_user)])],
        ["user_id", "distance", "paths"]
    )

    for _ in range(max_iter):
        target_reached = distances.filter(F.col("user_id") == target_user)
        if target_reached.count() > 0:
            return target_reached.select("user_id", "distance", F.explode("paths").alias("path"))

        temp = distances.join(followers_graph, "user_id", "inner")\
            .select(
                F.explode("followers").alias("follower"),
                (F.col("distance") + 1).alias("distance"),
                F.col("paths")
            )
        
        new_distances = temp.withColumn(
            "paths",
            F.expr("transform(paths, x -> concat(x, '->', follower))")
        ).withColumnRenamed("follower", "user_id")

        
        distances = distances.union(new_distances)\
            .groupBy("user_id")\
            .agg(
                F.min("distance").alias("distance"),
                F.collect_list("paths").alias("paths")
            ).withColumn("paths", F.flatten("paths"))  

    return distances.filter(F.col("user_id") == target_user).select("user_id", "distance",F.explode("paths").alias("path"))



log_schema = StructType(fields=[
    StructField("user_id", LongType()),
    StructField("follower_id", LongType()),
])

df = spark.read.csv(path_to_df, sep='\t', schema=log_schema)
ans = find_shortest_path(spark, df, finish, start)
ans.withColumn("reversed_path", F.expr("reverse(split(path, '->'))")) \
   .withColumn("reversed_path", F.expr("concat_ws(',', reversed_path)")) \
   .select("reversed_path") \
   .write.mode("overwrite").option("quote", "").csv(path_to_ans, header=False)

