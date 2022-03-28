from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/Week10_dataset/bigdatacampaigndata.csv")
rdd1=rdd.map(lambda x:(x.split(",")[0],float(x.split(",")[10])))
rdd2=rdd1.map(lambda x:(x[1],x[0].lower()))
rdd3=rdd2.flatMapValues(lambda x:x.split(" "))
rdd4=rdd3.map(lambda x:(x[1],x[0]))
rdd5=rdd4.reduceByKey(lambda x,y:x+y)
rdd6=rdd5.sortBy(lambda x:x[1],False)
res=rdd6.collect()

for i in res:
    print(i)
