from pyspark import SparkContext
import findspark
findspark.init()

sc=SparkContext("local[*]","app")
rdd=sc.textFile("D:/BigDataSumit/week9_datatset/search_data.txt")

#using map()+reduceByKey() and sortByKey()
rdd1=rdd.flatMap(lambda x:x.split(" "))
rdd2=rdd1.map(lambda x:(x.lower(),1))
rdd3=rdd2.reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0]))
rdd4=rdd3.sortByKey(False).map(lambda x:(x[1],x[0]))

res=rdd4.collect()  #collect returns a list of tuples
for i in res:
    print(i)


#using countByValue()

rdd5=rdd.flatMap(lambda x:x.split(" ")).map(lambda x:x.lower())
#rdd6=rdd5.countByValue()
#for i,j in rdd6.items():   #countByValue() returns a dict
#    print(i,j)


#using map() + reduceByKey() + sortBy()

rdd7=rdd.flatMap(lambda x:x.split(" "))
rdd8=rdd7.map(lambda x:(x.lower(),1))
rdd9=rdd8.reduceByKey(lambda x,y:x+y)
rdd10=rdd9.sortBy(lambda x:x[1],False)
#rdd11=rdd10.collect()   #collect returns a list of tuples

#for i in rdd11:
#    print(i)
