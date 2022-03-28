
#write a quetry to find personID,name,number of friends, sum of marks of person who have friends with total score greater than 100

from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

person_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/sql_playlist/person_table.csv")\
    .load()

friend_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/sql_playlist/friend_table.csv")\
    .load()

df1=person_df.alias("p").join(friend_df.alias("f"),person_df["PersonID"]==friend_df["PersonID"],"inner").\
    select("p.PersonID","Name","Email","Score","FriendID")
df1.join(person_df.alias("p1"),df1["FriendID"]==person_df["PersonID"],"inner").show()
