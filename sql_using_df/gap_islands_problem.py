#gap and islands problem
#https://community.oracle.com/tech/developers/discussion/4491739/need-help-with-sql-queries
# INPUT:
# Asin day is_instock
# A1    1     0
# A1    2     0
# A1    3     1
# A1    4     1
# A1    5     0
#
# Output:
# asin    start_day    end_day    is_instock
# a1        1                 2                   0
# a1        3                 4                   1
# a1        5                 5                   0


from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")


spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

lst=[['A1',1,0],['A1',2,0],['A1',3,1],['A1',4,1],['A1',5,0]]

df=spark.createDataFrame(lst).toDF("Asin","day","is_instock")

#df.show()
my_window=Window.orderBy("day")

df1=df.withColumn("prev_instock",lag("is_instock",1).over(my_window))

df2=df1.withColumn("change",expr("case when is_instock!=prev_instock then 1 else 0 end"))

df3=df2.withColumn("group_id",sum("change").over(my_window))

df4=df3.groupby("asin","group_id","is_instock")\
    .agg(min("day").alias("start_day"),
         max("day").alias("end_day"))

df4.select("asin","start_day","end_day","is_instock").orderBy("asin","start_day").show()