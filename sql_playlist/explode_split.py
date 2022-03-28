from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

lst=[["Yash","ACC-JIO"],["Vishal","ACC-MC-Fractal"],["Ankit","ACC-KPMG-Fractal"]]

df=spark.createDataFrame(lst,["name","companies"])


df.select("name",explode(split(df["companies"],"-"))).show()

df.select("name",explode(split(col("companies"),"-"))).show()