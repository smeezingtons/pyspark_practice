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

data=spark.sparkContext.textFile("D:/BigDataSumit/week9_datatset/wordcount.txt")
count=data.flatMap(lambda x:[(c,1) for c in x])
cnt=count.filter(lambda x:len(x[0].strip())!=0)
count1=cnt.reduceByKey(lambda x,y:x+y)


res=count1.sortBy(lambda x:x[1],False).collect()

for i in res:
    print(i)

