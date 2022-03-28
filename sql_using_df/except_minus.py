#https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-setops.html
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

lst1=[3,1,2,2,3,4]
lst2=[5,1,2,2]

df1=spark.createDataFrame(lst1,IntegerType()).toDF("c")
df2=spark.createDataFrame(lst2,IntegerType()).toDF("c")

df1.createOrReplaceTempView("number1")
df2.createOrReplaceTempView("number2")

#except returns the rows which are present in number1 table but not present in number2 table.
#except returns only unique values, it removes duplicates like union.
spark.sql("select c from number1 except select c from number2").show()

#except all keeps duplicates like union all
spark.sql("select c from number1 except all select c from number2").show()

#MINUS is an alias for EXCEPT, MINUS has exact same functionality as EXCEPT.
#MINUS removes duplicates
spark.sql("select c from number1 minus select c from number2").show()

#MINUS all keeps duplicates
spark.sql("select c from number1 minus all select c from number2").show()


#we can also use DF syntax for exceptAll
#DF syntax can't be used for except,minus all,minus in pyspark
res=df1.exceptAll(df2)
res.show()
