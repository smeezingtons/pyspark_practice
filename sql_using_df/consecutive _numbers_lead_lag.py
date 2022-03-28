#https://www.youtube.com/watch?v=xoUlPOcbBKA

from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

lst=[6,10,11,12,57,77,98,99,100,120,125]

df=spark.createDataFrame(lst,IntegerType()).toDF("num")

my_window=Window.orderBy("num")

df1=df.withColumn("next_num",lead("num",1).over(my_window)).withColumn("prev_num",lag("num",1).over(my_window))
df2=df1.withColumn("diff1",df1["next_num"]-df1["num"]).withColumn("diff2",df1["num"]-df1["prev_num"])
df2.where("diff1=1 or diff2=1").select("num").show()

#using spark SQL
df.createOrReplaceTempView("numbers")

spark.sql("""select num from (
select num,next_num,prev_num,next_num-num as diff1,num-prev_num as diff2 from (
select num,lead(num,1) over(order by num) as next_num,lag(num,1) over(order by num) as prev_num from numbers)) 
where diff1=1 or diff2=1""").show()