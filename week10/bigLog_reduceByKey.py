from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/Week10_dataset/bigLogtxt-201014-183159/bigLog.txt")
rdd1=rdd.map(lambda x:x.split(":")[0])
rdd2=rdd1.map(lambda x:(x,1))
rdd3=rdd2.reduceByKey(lambda x,y:x+y)

res=rdd3.collect()
for i in res:
    print(i)