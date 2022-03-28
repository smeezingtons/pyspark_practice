#How can you find 10 employees with Odd number as Employee ID?
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

emp_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/emp_table.csv")\
    .load()

emp_df.where("mod(empno,2)=1").show()

emp_df.createOrReplaceTempView("emp")

spark.sql("select * from emp where mod(empno,2)=1").show()