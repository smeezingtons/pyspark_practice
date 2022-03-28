from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/week9_datatset/wordcount.txt")

#There are two approaches - 1. using count 2. using map + reduce
rdd1=rdd.flatMap(lambda x:x.split(" "))
cnt=rdd1.count()
print(cnt)

rdd2=rdd.flatMap(lambda x:x.split(" "))
rdd3=rdd2.map(lambda x:1)
cnt1=rdd3.reduce(lambda x,y:x+y)
print(cnt1)