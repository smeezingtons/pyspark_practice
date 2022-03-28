# There is a list of countries say IND, PAK, CHN, AFG, SRI, BNG. Create a combination of countries with the help of this list using one query
# How about IND-PAK & PAK-IND duplicate.

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

lst=["IND","PAK","CHN","AFG","SRI","BNG"]

df=spark.createDataFrame(lst,StringType()).toDF("cntry")

df.alias("df1").crossJoin(df.alias("df2"))\
    .where("df1.cntry<df2.cntry")\
    .selectExpr("concat(df1.cntry,'-',df2.cntry)")\
    .show()


