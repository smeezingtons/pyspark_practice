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

my_window=Window.partitionBy("EMPNO").orderBy("ENAME")
#approach 1 : using row_number
rnk_df=emp_df.withColumn("rnk",row_number().over(my_window))
dedup_df=rnk_df.where("rnk=1")
dedup_df.show()

#approach 2 : using dropDuplicates
dedup_df1=emp_df.dropDuplicates(["EMPNO"])
dedup_df1.show()

dedup_df2=emp_df.drop_duplicates(["EMPNO"])
dedup_df2.show()
