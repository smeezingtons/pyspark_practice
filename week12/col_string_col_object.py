from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week12/orders.csv")\
    .load()

df.select("order_id","order_date","order_customer_id","order_status").show()  #column string

df.select(col("order_id"),col("order_date"),col("order_customer_id"),col("order_status")).show()  #column object
