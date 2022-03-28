from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/week9_datatset/friendsdata.csv")

rdd1=rdd.map(lambda x:(x.split("::")[2],int(x.split("::")[3])))

rdd2=rdd1.map(lambda x:(x[0],(x[1],1)))

rdd3=rdd2.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

rdd4=rdd3.map(lambda x:(x[0],x[1][0]/x[1][1]))

res=rdd4.sortBy(lambda x:x[1],False).collect()

for i in res:
    print(i)


