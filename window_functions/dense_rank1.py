from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import desc,dense_rank
from pyspark.sql.types import *

#show the name of employee who's earning the highest salary
spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

emp_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/emp_table.csv")\
    .load()

my_window=Window.orderBy(desc("sal")).rowsBetween(Window.unboundedPreceding,Window.currentRow)

rnk_df=emp_df.withColumn("rnk",dense_rank().over(my_window))

highest_earner=rnk_df.where("rnk=1").select("ENAME")

highest_earner.show()

#WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
#when you dont mention partitionBy in window then all data goes to only one partition