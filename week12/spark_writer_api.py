from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchea",True)\
    .option("path","D:/BigDataSumit/week_11_dataset/orders.csv")\
    .load()

df2=df.where("order_customer_id>1000")\
    .groupBy("order_customer_id")\
    .count().orderBy("count",ascending=False)

df2.write.format("csv")\
    .mode("append")\
    .option("path","D:/BigDataSumit/week_11_dataset/op_new/")\
    .save()