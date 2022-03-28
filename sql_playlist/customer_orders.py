#https://youtu.be/MpAMjtvarrc

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

order_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/sql_playlist/orders_table.csv")\
    .load()

first_order_df=order_df.groupby("CUSTOMER_ID")\
    .agg(min("ORDER_DATE").alias("first_order"))

joined_df=order_df.join(first_order_df,order_df['CUSTOMER_ID']==first_order_df['CUSTOMER_ID'],"inner")

df1=joined_df.withColumn("new_cust",expr("case when ORDER_DATE=first_order then 1 else 0 end"))\
    .withColumn("old_cust",expr("case when ORDER_DATE!=first_order then 1 else 0 end"))\
    .withColumn("new_cust_rev",expr("case when ORDER_DATE=first_order then ORDER_AMOUNT else 0 end"))\
    .withColumn("old_cust_rev",expr("case when ORDER_DATE!=first_order then ORDER_AMOUNT else 0 end"))

df1.groupby("ORDER_DATE")\
    .agg(sum("new_cust").alias("new_cust_cnt"),
         sum("old_cust").alias("old_cust_cnt"),
         sum("new_cust_rev"),
         sum("old_cust_rev")).orderBy("order_date").show()

order_df.createOrReplaceTempView("customer_orders");

spark.sql("""with first_order_cte as(
select customer_id,min(order_date) as first_order from customer_orders  group by customer_id)

select order_date,sum(new_cust),sum(old_cust),sum(new_cust_rev),sum(old_cust_rev) from (
select order_id,c.customer_id,order_date,f.first_order,
case when order_date=f.first_order then 1 else 0 end as new_cust,
case when order_date<>f.first_order then 1 else 0 end as old_cust,
case when order_date=f.first_order then ORDER_AMOUNT else 0 end as new_cust_rev,
case when order_date<>f.first_order then ORDER_AMOUNT else 0 end as old_cust_rev
from customer_orders c,first_order_cte f where f.customer_id=c.customer_id)
group by order_date order by order_date""").show()