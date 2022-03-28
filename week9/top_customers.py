from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/week9_datatset/customerorders.csv")
rdd1=rdd.map(lambda x:(x.split(",")[0],float(x.split(",")[2])))
rdd2=rdd1.reduceByKey(lambda x,y:x+y)
rdd3=rdd2.sortBy(lambda x:x[1],False)
rdd4=rdd3.collect()

for i in rdd4:
    print(i)