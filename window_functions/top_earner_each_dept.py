from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Write a SQL Query to find Max salary and Department name from each department.

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

emp_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/emp_table.csv")\
    .load()

dept_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/dept_table.csv")\
    .load()

max_df=emp_df.groupby("DEPTNO")\
    .agg(max("sal").alias("max_sal"))

#if youn dont drop deptno column from any one DF then you will get ambiguity error
joined_df=max_df.join(dept_df,max_df['deptno']==dept_df['deptno'],"inner").drop(dept_df["deptno"]).select("deptno","max_sal","name")
joined_df.show()



