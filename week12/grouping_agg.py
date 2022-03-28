from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week12/order_data.csv")\
    .load()

#column object expression
df2=df.groupBy("Country").\
    agg(count("*").alias("Country_wise_cnt"),sum("Quantity").alias("Country_wise_Quantity"))

df2.show()

#column string expression
df3=df.groupBy("Country").\
    agg(expr("count(*) as countyry_wise_cnt"),expr("sum(Quantity*UnitPrice) as InvoiveValue"))

df3.show()


df.createTempView("temp_table")

spark.sql("select country,count(*) as country_wise_cnt,sum(Quantity*UnitPrice) as InvoiveValue from temp_table group by country").show()


