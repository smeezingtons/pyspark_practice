#Write a SQL query to find records in Table A that are not in Table B without using NOT IN operator.

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

empA_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/emp_table_A.csv")\
    .load()

empB_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/emp_table_B.csv")\
    .load()

# approach 1: you can use anti join
res=empA_df.join(empB_df,empA_df["empno"]==empB_df["empno"],"anti")
#res.show()


empB_df.createOrReplaceTempView("empB_tbl")
empA_df.createOrReplaceTempView("empA_tbl")


#spark.sql("select a.* from empA_tbl a anti join empB_tbl b on a.empno=b.empno").show()

# approach 2: you can use left outer join
#To select columns from specific DF after join you have to give alias to DFs while kjoining them and then select columns using DFalias.column_name
res1=empA_df.alias("DF_A").join(empB_df.alias("DF_B"),empA_df["empno"]==empB_df["empno"],"left")\
    .where("DF_B.empno is null").select("DF_A.*")
#res1.show()

#spark.sql("select a.* from empA_tbl a left outer join empB_tbl b on a.empno=b.empno where b.empno is null").show()