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

lst1=[1,2,3,7]
lst2=[3,2,1,4,2,3,1]

df1=spark.createDataFrame(lst1,IntegerType()).toDF("key1")
df2=spark.createDataFrame(lst2,IntegerType()).toDF("key2")

# Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'.


#inner join
joinedDF1=df1.join(df2,df1.key1 == df2.key2,"inner")  #IDE shows it as syntax error but it works, we can use df.key1 or df["key1"}
joinedDF2=df1.join(df2,df1["key1"] == df2["key2"],"inner")

#joinedDF1.show()
#joinedDF2.show()

#left join
joinedDF3=df1.join(df2,df1.key1 == df2.key2,"left")  #IDE shows it as syntax error but it works, we can use df.key1 or df["key1"}
joinedDF4=df1.join(df2,df1["key1"] == df2["key2"],"left")

#joinedDF3.show()
#joinedDF4.show()

#right join

joinedDF5=df1.join(df2,df1.key1 == df2.key2,"right")  #IDE shows it as syntax error but it works, we can use df.key1 or df["key1"}
joinedDF6=df1.join(df2,df1["key1"] == df2["key2"],"right")

#joinedDF5.show()
#joinedDF6.show()

#full outer join

joinedDF7=df1.join(df2,df1.key1 == df2.key2,"full")  #IDE shows it as syntax error but it works, we can use df.key1 or df["key1"}
joinedDF8=df1.join(df2,df1["key1"] == df2["key2"],"right")

#joinedDF7.show()
#joinedDF8.show()


#left semi join
#leftsemi join is equivalent to intersect operator of oracle, it will give common records between two DFs.
#this should return only two rows as only 1,2,3 are common between two DFs.


joinedDF9=df1.join(df2,df1.key1 == df2.key2,"leftsemi")  #IDE shows it as syntax error but it works, we can use df.key1 or df["key1"}
joinedDF10=df1.join(df2,df1["key1"] == df2["key2"],"leftsemi")

#joinedDF9.show()
#joinedDF10.show()


#left anti
#leftanti join is equivalent to minus operator of oracle,  it will return records that are present in left side
#table but not present in right side table i.e left table - right table
#An anti join returns values from the left relation that has no match with the right. It is also referred to as a left anti join.


joinedDF11=df1.join(df2,df1.key1 == df2.key2,"anti")  #IDE shows it as syntax error but it works, we can use df.key1 or df["key1"}
joinedDF12=df1.join(df2,df1["key1"] == df2["key2"],"anti")  #leftanti and anti join is same

#joinedDF11.show()
#joinedDF12.show()

#cross join

joinedDF13=df1.crossJoin(df2)
joinedDF14=df1.join(df2)  #when you do join without any condition then it's a cross join.

joinedDF13.show()
joinedDF14.show()
