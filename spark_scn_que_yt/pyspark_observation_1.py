from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext

import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

#when you create RDD directly using sparkContext then you get rdd of string type i.e. RDD[str]
rdd=spark.sparkContext.textFile("D:/BigDataSumit/week_11_dataset/orders_new.csv")
res=rdd.collect()
print(res)

#when you create RDD from DF using .rdd then you get RDD of Row type i.e. RDD[Row], that's why we use lambda x:x[0].split(" ")
df7=spark.read.text("D:/BigDataSumit/week_11_dataset/orders_new.csv")
rdd2=df7.rdd.collect()
print(rdd2)