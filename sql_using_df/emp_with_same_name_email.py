#Write SQL Query to find employees that have same name and email.

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
    .option("path","D:/spark_dataset/emp_dup_name_email.csv")\
    .load()

#DF doesnt have having function so even for agg functions you use where function.
emp_df.groupby("email","ename")\
    .agg(count("*").alias("cnt"))\
    .where("cnt>1")\
    .select("ename","email")\
    .show()