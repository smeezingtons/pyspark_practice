from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import desc,row_number
from pyspark.sql.types import *

#remove dups from emp_table
spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

emp_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/emp_table.csv")\
    .load()

emp_df.explain(extended=True)

emp_df.explain('codegen')