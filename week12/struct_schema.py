from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

ordersDFSchema=StructType([StructField("order_id_new",IntegerType()),StructField("order_date",StringType()),
            StructField("order_customer_id",IntegerType()),StructField("order_status",StringType())])


df=spark.read.format("csv")\
    .option("header",True)\
    .schema(ordersDFSchema)\
    .option("path","D:/BigDataSumit/week12/orders.csv")\
    .load()

df.show()

df.printSchema()