#video link: https://youtu.be/CtkzPdS3pyU

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

#approach 1 : using delimiter option in reader API  (This works only in spark 3.0)
#in spark 2.x there was no support for multi delimiter.  #http://blog.madhukaraphatak.com/spark-3-introduction-part-1/

df=spark.read.format("csv")\
    .option("delimiter",'~|')\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/Youtube_spark_practice/multi_delim.txt")\
    .load()

df.show()


#approach 2 : using RDD approach
rdd=spark.sparkContext.textFile("D:/BigDataSumit/Youtube_spark_practice/multi_delim.txt")
header=rdd.first()
rdd1=rdd.filter(lambda x:x!=header).map(lambda x:(x.split("~|")[0],x.split("~|")[1]))
df1=spark.createDataFrame(rdd1).toDF("Name","Age")
df1.show()


#approach 3 : using DF and RDD
df2=spark.read.text("D:/BigDataSumit/Youtube_spark_practice/multi_delim.txt")
header1=df2.first()[0]
df3=df2.filter(df2["value"]!=header1)  #you can also use column object here i.e  df2.filter(col("value")!=header1)
df3.rdd.map(lambda x:(x[0].split("~|"))).toDF(["Name","Age"]).show()







