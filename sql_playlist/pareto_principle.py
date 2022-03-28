#https://youtu.be/oGgE180oaTs
#find the products which give 80% of the sales.

from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

orders_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/sql_playlist/Superstore_orders.csv")\
    .load()

per_product_sale=orders_df.groupby("Product_ID")\
    .agg(sum("Sales").alias("sales"))

my_window=Window.orderBy(desc("sales"))


df_running_sales=per_product_sale.withColumn("running_ttl",sum("sales").over(my_window))

my_window1=Window.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

df_total_sales=df_running_sales.withColumn("total_sales",sum("sales").over(my_window1))

df_80percnt_sales=df_total_sales.withColumn("80percent_sales",expr("total_sales*0.8"))
top_products=df_80percnt_sales.where("running_ttl<=80percent_sales")
top_products.show()
print(top_products.count())


#using spark SQL
orders_df.createOrReplaceTempView("orders")

spark.sql("""
select Product_ID,sales,running_ttl,total_sales from (
select Product_ID,sales,sum(sales) over(order by sales desc) as running_ttl,
sum(sales) over(rows between unbounded preceding and unbounded following) as total_sales  from (
select Product_ID,sum(sales) as sales from orders group by Product_ID))
where running_ttl<=(total_sales*0.8)

""").show()

