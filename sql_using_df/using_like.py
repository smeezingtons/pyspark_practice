#In the data of recipes [JSOn format], How would you extract the recipes which have water as one of their ingredients?

from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

recipe_df=spark.read.format("json")\
    .option("mode","PERMISSIVE")\
    .option("path","D:/spark_dataset/recipesjson.json")\
    .load()

#contains or like can be used

recipe_df.where(col("ingredients").like("%Water%")).show()

recipe_df.where("ingredients like '%water%' or ingredients like '%Water%'").show()

recipe_df.where(col("ingredients").contains("Water")).show()