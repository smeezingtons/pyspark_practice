from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/week9_datatset/wordcount.txt")

words=rdd.flatMap(lambda x:x.split(" "))
rdd2=words.map(lambda x:(x,1))
rdd3=rdd2.reduceByKey(lambda x,y:x+y)
wc=rdd3.collect()

for i in wc:
    print(i)
