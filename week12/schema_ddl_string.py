from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,IntegerType,StringType,DateType

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

orders_schema="orderId integer,order_date string,order_customer_id integer,order_status string"

df=spark.read.format("csv")\
    .option("header",True)\
    .schema(orders_schema)\
    .option("path","D:/BigDataSumit/week12/orders.csv")\
    .load()

df.show()

df.printSchema()