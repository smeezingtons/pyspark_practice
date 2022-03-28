from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,StructField,StructType


spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week12/orders.csv")\
    .load()


df.write.format("csv")\
    .mode("overwrite")\
    .option("maxRecordsPerFile",100)\
    .option("path","D:/BigDataSumit/week12/order_maxrec")\
    .save()