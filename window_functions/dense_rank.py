from pyspark import  SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import dense_rank,desc
from pyspark.sql.types import *

#find top 2 earners from each dept
spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

emp_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/emp_table.csv")\
    .load()

emp_df.show()

my_window=Window.partitionBy("DEPTNO").orderBy(desc("sal")).rowsBetween(Window.unboundedPreceding,Window.currentRow)

ranked_df=emp_df.withColumn("rnk",dense_rank().over(my_window))

ranked_df.show()

top_two=ranked_df.where("rnk<3")

top_two.show()