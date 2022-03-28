#video link:  https://youtu.be/aAIWlFzVpIA

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.text("D:/BigDataSumit/Youtube_spark_practice/emp.txt")
df.show(truncate=False)

df1=df.withColumn("chk",regexp_replace("value","(.*?\\|){5}","$0-"))
df1.show(truncate=False)

df2=df1.select(explode(split("chk","\|-")).alias("new_col"))

df2.show(truncate=False)


#approach 1 : Here you have to manually extract every column using index, this is wont work if you have huge no of columns.
df2.select(split("new_col","\|")[0],split("new_col","\|")[1],split("new_col","\|")[2],
           split("new_col","\|")[3],split("new_col","\|")[4]).toDF("Name","Education","No","tech","Ph_Number").show()


#approach 2: Here you convert DF into RDD and split the columns using | delimiter and then again create DF from it. This approch is better

df2.rdd.collect()  # This statement returns the following output:  [Row(new_col='Azar|BE|8|BigData|9273564531'), Row(new_col='Ramesh|BTech|3|Java|8433961222'), Row(new_col='Parthiban|ME|6|dotNet|8876534121'), Row(new_col='Mangesh|MCA|8|DBA|9023451789')]
                       #That's why we need to use x[0] instead of x

df2.rdd.map(lambda x:x).collect() # This statement returns the following output:  [Row(new_col='Azar|BE|8|BigData|9273564531'), Row(new_col='Ramesh|BTech|3|Java|8433961222'), Row(new_col='Parthiban|ME|6|dotNet|8876534121'), Row(new_col='Mangesh|MCA|8|DBA|9023451789')]

df2.rdd.map(lambda x:x[0]).collect() # This statement returns the following output: ['Azar|BE|8|BigData|9273564531', 'Ramesh|BTech|3|Java|8433961222', 'Parthiban|ME|6|dotNet|8876534121', 'Mangesh|MCA|8|DBA|9023451789']

rdd=df2.rdd.map(lambda x:x[0].split("|"))

spark.createDataFrame(rdd).toDF("Name","Education","No","tech","Ph_Number").show()




