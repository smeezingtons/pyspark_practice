#Write a SQL Query to get the names of employees whose date of HIREDATE is between 01-JAN-80 to 30-DEC-81.

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

emp_schema=StructType([StructField("EMPNO",IntegerType()),StructField("ENAME",StringType()),StructField("JOB",StringType()),
                       StructField("MGR",IntegerType()),StructField("HIREDATE",DateType()),StructField("SAL",IntegerType()),
                       StructField("COMM",IntegerType()),StructField("DEPTNO",IntegerType())])




emp_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .schema(emp_schema)\
    .option("path","D:/spark_dataset/emp_table.csv")\
    .load()
emp_df.printSchema()

emp_df.show()

#we have to use between function for this.
emp_df.filter(emp_df["hiredate"].between('01-JAN-80','30-DEC-81')).show()