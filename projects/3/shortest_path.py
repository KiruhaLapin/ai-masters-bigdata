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

def find_shortest_path(spark, edges_df, start_user, target_user, max_iter=1000000):
    """
    Находит кратчайший путь от start_user до target_user с ранней остановкой
    :param spark: SparkSession
    :param edges_df: DataFrame с колонками [user_id, follower_id]
    :param start_user: ID начального пользователя
    :param target_user: ID целевого пользователя
    :param max_iter: максимальное число итераций
    :return: DataFrame с одной строкой [user_id, distance, path] или пустой, если путь не найден
    """
    followers_graph = edges_df.groupBy("user_id")\
                             .agg(F.collect_list("follower_id").alias("followers"))
    
    distances = spark.createDataFrame(
        [(start_user, 0, str(start_user))],
        ["user_id", "distance", "path"]
    )
    for iteration in range(max_iter):
        target_reached = distances.filter(F.col("user_id") == target_user)
        if target_reached.count() > 0:
            return target_reached
        
        temp = distances.join(followers_graph, "user_id", "inner")\
            .select(
                F.explode("followers").alias("follower"),
                (F.col("distance") + 1).alias("distance"),
                F.col("path")
            )
        
        new_distances = temp.withColumn(
            "path",
            F.concat(F.col("path"), F.lit("->"), F.col("follower").cast("string"))
        ).withColumnRenamed("follower", "user_id")
        
        distances = distances.union(new_distances)\
            .groupBy("user_id")\
            .agg(
                F.min("distance").alias("distance"),
                F.first("path").alias("path")
            )
 
        
    
    return distances.filter(F.col("user_id") == target_user)


log_schema = StructType(fields=[
    StructField("user_id", LongType()),
    StructField("follower_id", LongType()),
])

df = spark.read.csv(path_to_df, sep='\t', schema=log_schema)
ans = find_shortest_path(spark, df, finish, start)
ans.withColumn("reversed_path", F.expr("reverse(split(path, '->'))")) \
   .withColumn("reversed_path", F.expr("concat_ws(',', reversed_path)")) \
   .select("reversed_path") \
   .write.mode("overwrite").csv(path_to_ans, header=False)

