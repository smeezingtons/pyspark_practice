#https://youtu.be/qyAgWL066Vo

#input table:
# Team1 Team2 MatchResult
#  RR KKR 2
#  MI CSK 2
#  RCB KXP 1
#  DD RR 0
#  KKR RR 1
#  CSK RCB 2
#  KXP DD 2
#
#  Match Result descriptions:
#  1 => Match won by Team 1
#  2 => Match won by Team 2
#  0 => Draw
#
# Output should have following columns
#
# Team Played Won Lost Draw
# RR 3 0 2  1
# CSK 2 1 1  0
# RCB 2 2 0  0

from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","app")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

teams_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/spark_dataset/lumiq/teams_table.csv")\
    .load()


team_1df=teams_df.withColumn("res",expr("case when MatchResult=1 then 1 else 0 end")).\
    withColumn("draw",expr("case when MatchResult=0 then 1 else 0 end")).select("team1","res","draw")

team_2df=teams_df.withColumn("res",expr("case when MatchResult=2 then 1 else 0 end")).\
    withColumn("draw",expr("case when MatchResult=0 then 1 else 0 end")).select("team2","res","draw")

res_df=team_1df.unionAll(team_2df).toDF("team","res","draw")

res_df.groupby("team")\
    .agg(count("team").alias("matches_played"),
         sum("res").alias("wins"),
         (count("team")-sum("res")-sum("draw")).alias("losses"),
         sum("draw").alias("draws")).show()

teams_df.createOrReplaceTempView("teams")

spark.sql("""select team,count(1) as matches_played,sum(res) as won,count(1)-sum(res)-sum(draw) as lost,sum(draw) as draw from (
select team1 as team,case when MATCHRESULT=1 then 1 else 0 end as res,case when MATCHRESULT=0 then 1 else 0 end as draw from teams
union all
select team2 as team,case when MATCHRESULT=2 then 1 else 0 end as res,case when MATCHRESULT=0 then 1 else 0 end as draw from teams)
group by team""").show()