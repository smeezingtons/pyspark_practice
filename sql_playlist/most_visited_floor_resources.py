#https://youtu.be/P6kNMyqKD0A
from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

order_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/sql_playlist/entries.csv")\
    .load()

order_df1=order_df.groupby("name","floor")\
    .agg(count("floor").alias("cnt"))

my_window=Window.partitionBy("name").orderBy(desc("cnt"))

max_floor_visit=order_df1.withColumn("rnk",dense_rank().over(my_window)).where("rnk=1").select("name","floor")


total_visit_resources=order_df.groupby("name").\
    agg(count("ADDRESS").alias("total_visit"),
        collect_list("RESOURCES").alias("RESOURCES"))

max_floor_visit.alias("f").join(total_visit_resources.alias("vr"),max_floor_visit["name"]==total_visit_resources["name"],"inner")\
    .select("vr.name","f.floor","vr.total_visit","RESOURCES").show()


#using spark SQL
order_df.createOrReplaceTempView("entries")

spark.sql("""with floor_visit as (
select name,floor as max_visited_floor from (
select name,floor,dense_rank() over (partition by name order by no_of_times_visit desc) as rnk from (
select name,floor,count(1) as no_of_times_visit from entries
group by name,floor) ) where rnk=1)

select e.name,count(1) as total_visits,f.max_visited_floor as most_visited_floor,collect_list(RESOURCES) from entries e,floor_visit f where e.name=f.name 
group by e.name,f.max_visited_floor""").show()


#oracle query (in oracle we have listagg() function insted of collect_list())
# with floor_visit as (
# select name,floor as max_visited_floor from (
# select name,floor,dense_rank() over (partition by name order by no_of_times_visit desc) as rnk from (
# select name,floor,count(1) as no_of_times_visit from entries
# group by name,floor) ) where rnk=1)
#
# select e.name,count(1) as total_visits,f.max_visited_floor as most_visited_floor,listagg(distinct RESOURCES,',') from entries e,floor_visit f where e.name=f.name
# group by e.name,f.max_visited_floor;