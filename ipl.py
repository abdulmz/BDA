from pyspark.sql.functions import *

rawDF = spark.read.json("/home/abdulmueez/data/ipl/xxx.json", multiLine = "true")

innDF = rawDF.select(explode(“innings.overs”))
delDF = innDF.select(explode(“col.deliveries”))
tableDF = delDF.select("col.batter","col.bowler",col("col.runs.batter").alias("linage"))

batterlist = tableDF.select(explode(“batter”).alias(“batter”))
bowlerlist = tableDF.select(explode("bowler"))
runslist = tableDF.select(explode("lineage”))

simpletableDF = batterlist.join(bowlerlist).join(runslist)
simpletableDF.count()
simpletableDF = simpletableDF.filter(simpletableDF.run!=0)
simpletableDF.count()

statDF = simpletableDF.groupby(‘batter’,’bowler’).sum(‘run’).orderBy(‘batter’)
statmaxDF = statDF.groupby(‘batter’,’bowler’).agg({‘sum(run)’:’max’})
resultDF = statmaxDF.select(‘batter’,’bowler’)

resultDF.show()