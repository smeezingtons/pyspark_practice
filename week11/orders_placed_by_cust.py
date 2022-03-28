from pyspark.sql import SparkSession
from pyspark import SparkConf

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week_11_dataset/orders.csv")\
    .load()

df2=df.where("order_customer_id>1000").\
    groupby("order_customer_id").\
    count().orderBy("count",ascending=False)


df2.show()