from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/BigDataSumit/week12/dataset1.csv")\
    .load()


def age_check(age):
    if age >= 18:
        return "Y"
    else:
        return "N"

df1=df.toDF("Name","age","location")

#column object expression
ageCheckerUdf=udf(age_check,StringType())

df2=df1.withColumn("Adult",ageCheckerUdf("age"))

df2.show()

#SQL expression
spark.udf.register("ageCheckerUdf1",age_check,StringType())

df3=df1.withColumn("Adult",expr("ageCheckerUdf1(age)"))

df3.show()

df1.createTempView("emp_table")

df4=spark.sql("select name,age,location,ageCheckerUdf1(age) as adult from emp_table")

df4.show()