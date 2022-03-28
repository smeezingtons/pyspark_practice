from pyspark import SparkContext
import findspark
findspark.init()


sc=SparkContext("local[*]","app")
blankLines=sc.accumulator(0)

def blank_lines_checker(line):
    if len(line)==0:
        blankLines.add(1)

rdd=sc.textFile("D:/BigDataSumit/Week10_dataset/samplefile.txt")
rdd.foreach(lambda x:blank_lines_checker(x))

print(blankLines.value)