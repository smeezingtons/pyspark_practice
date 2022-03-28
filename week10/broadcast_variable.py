from pyspark import SparkContext
import findspark
findspark.init()
sc=SparkContext("local[*]","app")


bwSet=set(i.strip() for i in open("D:\BigDataSumit\Week10_dataset/boringwords.txt"))

bwBroadcast=sc.broadcast(bwSet)

rdd=sc.textFile("D:/BigDataSumit/Week10_dataset/bigdatacampaigndata.csv")
rdd1=rdd.map(lambda x:(x.split(",")[0],float(x.split(",")[10])))
rdd2=rdd1.map(lambda x:(x[1],x[0]))
rdd3=rdd2.flatMapValues(lambda x:x.split(" "))
rdd4=rdd3.map(lambda x:(x[1].lower(),x[0]))
rdd5=rdd4.filter(lambda x:x[0] not in bwBroadcast.value)  #bwBroadcast.value returns a set
rdd6=rdd5.reduceByKey(lambda x,y:x+y)
rdd7=rdd6.sortBy(lambda x:x[1],False).collect()

for i in rdd7:
    print(i)





