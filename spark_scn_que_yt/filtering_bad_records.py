#video link: https://youtu.be/ThlpLhZCUtc

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

#To filter out bad records we use dropMalformed readmode of spark reader API.
#we have 3 modes: 1.permissive, 2.dropMalformed 3.failFast
df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("mode","dropMalformed")\
    .option("path","D:/BigDataSumit/Youtube_spark_practice/input1.csv")\
    .load()

df.show()