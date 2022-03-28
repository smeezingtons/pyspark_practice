#We need to check each gamer’s total cumulative score for each day in two different games.

from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

gamer_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/games_table.csv")\
    .load()


my_window=Window.partitionBy("game_id","gamer_id").orderBy("competition_date")

res=gamer_df.withColumn("total_score",sum("score").over(my_window))
res.show()