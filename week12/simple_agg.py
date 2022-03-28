from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

schema_ddl="""InvoiceNo string ,StockCode string, Description string, Quantity string,
                InvoiceDate string,UnitPrice string,CustomerID string,Country string"""

df=spark.read.format("csv")\
    .option("header",True)\
    .schema(schema_ddl)\
    .option("path","D:/BigDataSumit/week12/order_data.csv")\
    .load()

df.printSchema()
#df.show()

#simple object expression
df.select(
count("*").alias("RowCount"),
sum("Quantity").alias("Total_quantity"),
avg("UnitPrice").alias("avgPrice"),
countDistinct("InvoiceNo").alias("Distinct_cnt")
).show()

#column string expression
df.selectExpr(
"count(*) as RowCount",
"sum(Quantity) as Total_quantity",
"avg(UnitPrice) as avgPrice",
"count(distinct(InvoiceNo)) as distinct_invoice_cnt"
).show()


#spark SQL
df.createTempView("temp_table")

spark.sql("""select count(*) as RowCount,sum(Quantity) as Total_quantity,
            avg(UnitPrice) as avgPrice,count(distinct(InvoiceNo)) as distinct_cnt from temp_table""").show()

