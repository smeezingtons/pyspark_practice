from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week12/orders.csv")\
    .load()

spark.sql("create database if not exists retail")

df.write.format("csv")\
    .mode("overwrite")\
    .bucketBy(5,"order_id")\
    .sortBy("order_id")\
    .saveAsTable("retail.orders")