from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/week9_datatset/moviedata.data")
rdd1=rdd.map(lambda x:int(x.split("\t")[2]))
res=rdd1.countByValue()

for i in res.items():
    print(i)
