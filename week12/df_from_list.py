from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import Row,IntegerType,StructType,StructField
from pyspark.sql.functions import *
import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()


#creating DF from local nested list
lst1 = [["java", "dbms", "python"], ["OOPS", "SQL", "Machine Learning"]]  #this is our data
schema=["sub1","sub2","sub3"] # this is schema
df1=spark.createDataFrame(lst1,schema) #you have to pass data i.e lst1 as first arg to createDataFrame() method, schema is optional
df1.show()

#creating DF from normal list
lst2=[1,2,3]
df2=spark.createDataFrame(lst2,IntegerType()) #Here you have to give the datatype i.e IntegerType or else it gives error
df2.show()


#in pyspark you can't use toDF() method to convert local variable into DF, you get following error : Unresolved attribute reference 'toDF' for class 'list'
#spark.implicits is not available in pyspark
#This probably comes from the fact that spark.implicits._ uses the implicit type def in scala, which is a concept that do not exist in python.
