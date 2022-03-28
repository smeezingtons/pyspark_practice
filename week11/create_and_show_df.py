from pyspark.sql import SparkSession
from pyspark import SparkConf
import findspark
findspark.init()

sConf=SparkConf()
sConf.set("spark.app.name","app")
sConf.set("spark.master","local[*]")

spark=SparkSession.builder \
    .config(conf=sConf) \
    .enableHiveSupport()\
    .getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week_11_dataset/orders.csv")\
    .load()

df.show(truncate=False)