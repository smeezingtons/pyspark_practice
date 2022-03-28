from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import findspark
findspark.init()

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

myList = [(1,"2013-07-25",11599,"CLOSED"),(2,"2014-07-25",256,"PENDING_PAYMENT"),
          (3,"2013-07-25",11599,"COMPLETE"),(4,"2019-07-25",8827,"CLOSED")]

df=spark.createDataFrame(myList).toDF("order_id","order_date","order_customer_id","order_status")

df1=df.withColumn("date1",unix_timestamp(col("order_date")))\
    .withColumn("id",monotonically_increasing_id())\
    .dropDuplicates(["order_id","order_customer_id"])\
    .drop("order_id")\
    .sort("order_date")

df1.show()

df2=df.withColumn("date1",expr("concat(order_date,_10)"))\
    .withColumn("id",monotonically_increasing_id())\
    .dropDuplicates(["order_id","order_customer_id"])\
    .drop("order_id")\
    .sort("order_date")