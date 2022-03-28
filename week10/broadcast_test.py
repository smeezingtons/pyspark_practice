from pyspark import SparkContext
import findspark
findspark.init()
sc=SparkContext("local[*]","app")

bwSet=set(i.strip() for i in open("D:\BigDataSumit\Week10_dataset/boringwords.txt"))

bwBroadcast=sc.broadcast(bwSet)
print(bwBroadcast.value)