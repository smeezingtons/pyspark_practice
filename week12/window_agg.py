from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",False)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week12/windowdata-201025-223502.csv")\
    .load()

df1=df.toDF("country","weeknum","invoice_id","customer_id","invoicevalue")

myWindow=Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding,Window.currentRow)

df2=df1.withColumn("Running_total",sum(col("invoicevalue")).over(myWindow))

df2.show()