from pyspark import SparkContext
import findspark
findspark.init()


logList=["ERROR: Thu Jun 04 10:37:51 BST 2015",
"WARN: Sun Nov 06 10:37:51 GMT 2016",
"WARN: Mon Aug 29 10:37:51 BST 2016",
"ERROR: Thu Dec 10 10:37:51 GMT 2015",
"ERROR: Fri Dec 26 10:37:51 GMT 2014",
"ERROR: Thu Feb 02 10:37:51 GMT 2017"]

sc=SparkContext("local[*]","app")
rdd=sc.parallelize(logList)
rdd1=rdd.map(lambda x:x.split(":")[0])
#rdd2=rdd1.countByValue()
rdd3=rdd1.map(lambda x:(x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y).collect()

#for i,j in rdd2.items():
#    print(i,j)

for i in rdd4:
    print(i)