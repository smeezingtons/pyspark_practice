from pyspark import SparkContext
from functools import reduce
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/Week10_dataset/bigLogtxt-201014-183159/bigLog.txt")
rdd1=rdd.map(lambda x:(x.split(":")[0],1))
rdd2=rdd1.groupByKey()
#rdd3=rdd2.map(lambda x:(x[0],reduce(lambda x,y:x+y,x[1])))  # this can also be used, this will aggregate all 1s and give you total
rdd3=rdd2.map(lambda x:(x[0],len(x[1]))) #this also works, both approaches give the same result
rdd4=rdd3.collect()
for i in rdd4:
    print(i)