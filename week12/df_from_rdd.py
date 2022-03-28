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

#creating DF from RDD with only one column
#for RDD with only 1 column you can't use spark.createDataFrame method or toDF() method.
lst1=[1,2,3]
#no solution

#creating DF from RDD with multiple columns
#for RDD with multiple columns you can use both spark.createDataFrame and toDF() method.
#giving schema is optional when using spark.createDataFrame, schema can be given using structType or list
lst2 = [["java", "dbms", "python"], ["OOPS", "SQL", "Machine Learning"]]
rdd2=spark.sparkContext.parallelize(lst2)
df2=spark.createDataFrame(rdd2,["sub1","sub2","sub3"])
df2.show()
df3=rdd2.toDF()
df3.show()

#diff between toDF() and spark.createDataFrame() method
#1. toDF()
#in toDF() method we don’t have control over column type and nullable flag. (spark automatically decides datatype by reading the data in column)
#Mean’s there is no control over the schema customization. In most of the cases,
#toDF() method is only suitable for local testing.

#2.spark.createDataFrame()
#The createDataFrame() method addresses the limitations of the toDF() method.
#with createDataFrame() method we have control over complete schema customization, we can use structType to provide the schema to DF.