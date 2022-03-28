from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract


spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

myregex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

lines_df=spark.read.text("D:/BigDataSumit/week12/orders_unstrc.txt")

df=lines_df.select(regexp_extract("value",myregex,1).alias("order_id"),regexp_extract("value",myregex,2).alias("order_date"),
                regexp_extract("value",myregex,3).alias("order_customer_id"),regexp_extract("value",myregex,4).alias("order_status"))

df.show()

