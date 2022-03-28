#Create table of fixtures from below table of countries
#Country
#Ind
#Aus
#SA

#Result:
#c1 | c2
#ind | aus
#aus | sa
#sa | ind


from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

cntry_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/country_table.csv")\
    .load()

joined_df=cntry_df.alias("c1").crossJoin(cntry_df.alias("c2"))

res=joined_df.where("c1.Country<c2.Country")

res.show()
