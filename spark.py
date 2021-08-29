import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import split, explode
spark = SparkSession.builder.getOrCreate() #creates a spark session
rawDF = spark.read.json("/home/abdulmueez/bda/data/ipl_male_json/1082591.json", multiLine = "true")
rawDF.printSchema()

print("First Innings ")
print("iDF1=rawDF.select(f.col('innings').getItem(0).alias('innings1'))")
iDF1=rawDF.select(f.col('innings').getItem(0).alias('innings1'))
iDF1.show() #prints data in single line

print("displaying overs")
oDF1=iDF1.select(explode('innings1.overs').alias('overs'))
oDF1.show()

print("create a row for each element in the array ")
dDF1=oDF1.select(explode('overs.deliveries').alias('deliveries'))
dDF1.show()

print("Displaying each batter and bowler count") #for each ball
rsDF1=dDF1.select('deliveries.batter','deliveries.bowler',f.col('deliveries.runs.batter').alias("runs_scored"))
rsDF1.show()
print("Displaying total runs scored") #for each bowler
trDF1=rsDF1.groupBy('batter','bowler').agg(f.sum('runs_scored').alias('TotalRunsScored'))
trDF1.show()
print("Maximun runs scored") 
mrsDF1=trDF1.groupBy('batter').agg(f.max('TotalRunsScored').alias('MaxRunsScored'))
mrsDF1.show()
print("maximun runs scored among all bowlers") #maximum runs scored by a batter for a particular bowler
resDF1=trDF1.join(mrsDF1, (mrsDF1.MaxRunsScored==trDF1.TotalRunsScored) & (mrsDF1.batter==trDF1.batter), how="leftsemi").withColumnRenamed("TotalRunsScored", "MaxRunsScored")
resDF1.show()

print("Second Innings ")
iDF2=rawDF.select(f.col('innings').getItem(1).alias('innings2'))
iDF2.show()

print("displaying overs")
oDF2=iDF2.select(explode('innings2.overs').alias('overs'))
oDF2.show()

print("create a row for each element in the array ")
dDF2=oDF2.select(explode('overs.deliveries').alias('deliveries'))
dDF2.show()

print("Displaying each batter and bowler count")
rsDF2=dDF2.select('deliveries.batter','deliveries.bowler',f.col('deliveries.runs.batter').alias("runs_scored"))
rsDF2.show()

print("Displaying total runs scored")
TotalRunsDF=rsDF2.groupBy('batter','bowler').agg(f.sum('runs_scored').alias('TotalRunsScored'))
TotalRunsDF.show()

print("Maximun runs scored")
MaxRunsDF=TotalRunsDF.groupBy('batter').agg(f.max('TotalRunsScored').alias('MaxRunsScored'))
MaxRunsDF.show()

print("maximun runs scored among all bowlers")
resultDF=TotalRunsDF.join(MaxRunsDF, (MaxRunsDF.MaxRunsScored==TotalRunsDF.TotalRunsScored) & (MaxRunsDF.batter==TotalRunsDF.batter), how="leftsemi").withColumnRenamed("TotalRunsScored", "MaxRunsScored")
resultDF.show()
