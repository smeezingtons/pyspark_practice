from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/week_11_dataset/ratings.dat")
rdd1=rdd.map(lambda x:(x.split("::")[1],int(x.split("::")[2])))
rdd2=rdd1.map(lambda x:(x[0],(x[1],1)))
rdd3=rdd2.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd4=rdd3.filter(lambda x:x[1][1]>=1000)
rdd5=rdd4.map(lambda x:(x[0],x[1][0]/x[1][1]))
rdd6=rdd5.filter(lambda x:x[1]>=4.5)


mvrdd=sc.textFile("D:/BigDataSumit/week_11_dataset/movies.dat")
mvrdd1=mvrdd.map(lambda x:(x.split("::")[0],x.split("::")[1],x.split("::")[2]))
mvrdd2=mvrdd1.join(rdd6).map(lambda x:(x[1][0],x[1][1]))
res=mvrdd2.collect()
for i in res:
    print(i)