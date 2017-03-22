# -*- coding: utf-8 -*-
# 
import sys
#os.environ["SPARK_HOME"] = "/home/nisarg/spark-1.5.0-bin-hadoop2.6"
# Initialize a SparkContext
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pandas as pd 
from pyspark.sql.types import *
from textblob import TextBlob
import unicodedata
import re
#sc = SparkContext()
sqlContext = SQLContext(sc)

def compare(text,compare):
    a = compare
    found_a_string = False
    valuereturn = " "
    for item in a: 
      found_a_string = False
      if item.lower() in text and item != 'stark' and item != 'the'  :
        found_a_string = True
      if found_a_string:
         valuereturn = item
         return valuereturn
      else:
       valuereturn = ""
    return valuereturn   

conf = SparkConf().setMaster("local[*]").setAppName("GOTAnalysis")
sc = SparkContext(conf = conf)

# Import full dataset of newsgroup posts as text file
#data_raw = sc.textFile('/2011_12_15_twitter.csv').map(lambda line: line.split(";")).filter(lambda line: len(line)>1)
#dftweets = sqlContext.read.format("/2011_12_15_twitter.csv").option("header", "true").option("inferSchema", "true").load("data.csv")

data_raw = sc.textFile("/*r.csv").map(lambda line: line.split(";")).filter(lambda line: len(line)>1)
dftweets = data_raw.map(lambda line: (line[1],line[5],line[6],line[9]))
schema = StructType([StructField("date", StringType(), True),StructField("mentions", StringType(), True),StructField("hashtags", StringType(), True),StructField("text", StringType(), True)])
dftweetsfromPan = sqlContext.createDataFrame(dftweets,schema)
dftweetsfromPan.registerTempTable("tweets")
dftweetsfromPan.cache()

tweetsdeadtweets = sqlContext.sql("SELECT (CASE WHEN SUBSTR(date,0,4) = '2011' THEN 'Season1' WHEN SUBSTR(date,0,4) = '2012' THEN 'Season2' WHEN SUBSTR(date,0,4) = '2013' THEN 'Season3' WHEN SUBSTR(date,0,4) = '2014' THEN 'Season4' WHEN SUBSTR(date,0,4) = '2015' THEN 'Season5' ELSE  'Season6' END) AS GOTSeason,text  FROM tweets WHERE (text LIKE '%dead%') OR (text LIKE '%poison%') OR (text LIKE '%die%') OR (text LIKE '%prosecute%') OR (text LIKE '%behead%') OR (text LIKE '%decease%') OR (text LIKE '%execute%') OR (text LIKE '%strangle%') OR (text LIKE '%suicide%') OR (text LIKE '%assassinat%') OR (text LIKE '%stabbed%') OR (text LIKE '%incinerate%') OR (text LIKE '%murder%')")
##Reading dead charcater names
data_raw_Char = sc.textFile('/GOTCharactersDead.csv').map(lambda line: line.split(",")).filter(lambda line: len(line)>1)
data_raw_Char = data_raw_Char.map(lambda line: (line[3],line[13]))
schema_char = StructType([StructField("death", StringType(), True),StructField("GOTCharacter", StringType(), True)])
dfCharacters = sqlContext.createDataFrame(data_raw_Char,schema_char)
dfCharacters.registerTempTable("DeadChar")
##Reading alive charcaters
data_raw_Char_Alive = sc.textFile('/GOTCharactersAlive.csv').map(lambda line: line.split(",")).filter(lambda line: len(line)>1)
data_raw_Char_Alive = data_raw_Char_Alive.map(lambda line: (line[3],line[17]))
schema_char_Alive = StructType([StructField("Status", StringType(), True),StructField("GOTCharacter", StringType(), True)])
dfCharacters_Alive = sqlContext.createDataFrame(data_raw_Char_Alive,schema_char_Alive)
dfCharacters_Alive.registerTempTable("AliveChar")


tweetsdeadcharac = sqlContext.sql(" SELECT (CASE WHEN INSTR(GOTCharacter,' ')>0 THEN SUBSTR(GOTCharacter,0,INSTR(GOTCharacter,' ')) ELSE GOTCharacter END) AS NAME FROM DeadChar WHERE GOTCharacter <> 'GOTCharacter' ")

Searchfor = tweetsdeadcharac.select('NAME').rdd.flatMap(lambda x: [str(word.replace('"','').strip()) for word in x]).collect()
dfdead = tweetsdeadtweets.select('GOTSeason','text').rdd.map(lambda x: (x[0],x[1]))
Deadchar = dfdead.map(lambda x: (x[0],compare(x[1],Searchfor))).filter(lambda p: (len(p[1]) >1))
schema_Death_Pop = StructType([StructField("GOTSeason", StringType(), True),StructField("GOTCharacter", StringType(), True)])
DeathPopDF = sqlContext.createDataFrame(Deadchar,schema_Death_Pop)
DeathPopDF.registerTempTable("DeadPop")
DeathPopDFToWrite = sqlContext.sql("SELECT GOTSeason,GOTCharacter,count(GOTCharacter) AS DeadCount FROM DeadPop GROUP BY GOTSeason,GOTCharacter ")

#DeathPopDFToWrite.write.csv('/PopularDeaths.csv')
DeathPopDFToWrite.toPandas().to_csv('PopularDeaths.csv')

###Liking of character


tweetsLiking = sqlContext.sql("SELECT (CASE WHEN SUBSTR(date,0,4) = '2011' THEN 'Season1' WHEN SUBSTR(date,0,4) = '2012' THEN 'Season2' WHEN SUBSTR(date,0,4) = '2013' THEN 'Season3' WHEN SUBSTR(date,0,4) = '2014' THEN 'Season4' WHEN SUBSTR(date,0,4) = '2015' THEN 'Season5' ELSE  'Season6' END) AS GOTSeason,text  FROM tweets ")
combinedCharNames = sqlContext.sql(" (SELECT (CASE WHEN INSTR(GOTCharacter,' ')>0 THEN SUBSTR(GOTCharacter,0,INSTR(GOTCharacter,' ')) ELSE GOTCharacter END) AS NAME FROM DeadChar WHERE GOTCharacter <> 'GOTCharacter' ) UNION ALL (SELECT (CASE WHEN INSTR(GOTCharacter,' ')>0 THEN SUBSTR(GOTCharacter,0,INSTR(GOTCharacter,' ')) ELSE GOTCharacter END) AS NAME FROM AliveChar WHERE GOTCharacter <> 'GOTCharacter') ")
SearchforAll = combinedCharNames.select('NAME').rdd.flatMap(lambda x: [str(word.replace('"','').strip()) for word in x]).collect()
dfliking = tweetsLiking.select('GOTSeason','text').rdd.map(lambda x: (x[0],x[1]))
LikedCharacters = dfliking.map(lambda x: (x[0],x[1],compare(x[1],SearchforAll))) 
##.filter(lambda p: (len(p[1]) >1))
##popular charcaters
schema_pop_char = StructType([StructField("GOTSeason", StringType(), True),StructField("text", StringType(), True),StructField("GOTCharacter", StringType(), True)])
PopDF = sqlContext.createDataFrame(LikedCharacters,schema_pop_char)
PopDF.registerTempTable("PopChar")
PopDFToWrite = sqlContext.sql("SELECT GOTSeason,GOTCharacter,count(GOTCharacter) AS COUNTCharacters FROM PopChar WHERE length(GOTCharacter)>1 GROUP BY GOTSeason,GOTCharacter ")

PopDFToWrite.toPandas().to_csv('PopularCharacters.csv')

##Interest peak at what time – across years and months
interestPeak = sqlContext.sql("SELECT concat(concat(Year(to_date(date)),(CASE WHEN length(concat(Month(to_date(date)),'')) >1 THEN Month(to_date(date)) ELSE  concat('0',Month(to_date(date))) END )),(CASE WHEN length(concat(dayofmonth(to_date(date)),'')) >1 THEN dayofmonth(to_date(date)) ELSE  concat('0',dayofmonth(to_date(date))) END ))  AS Month_Year,count(text) AS Interest  FROM tweets GROUP BY concat(concat(Year(to_date(date)),(CASE WHEN length(concat(Month(to_date(date)),'')) >1 THEN Month(to_date(date)) ELSE  concat('0',Month(to_date(date))) END )),(CASE WHEN length(concat(dayofmonth(to_date(date)),'')) >1 THEN dayofmonth(to_date(date)) ELSE  concat('0',dayofmonth(to_date(date))) END )) ")
interestPeak.toPandas().to_csv('InterestPeak.csv')

##Sentiment analysis and polarity score
#clean tweets
regex_str = "http[s]?:// (?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+|(?:@[\w_]+)|(?:\#+[\w_]+[\w\'_\-]*[\w_]+)|http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+|(?:(?:\d+,?)+(?:\.?\d+)?)|(-)"
schema_Sentiment = StructType([StructField("Season", StringType(), True),StructField("Polarity", StringType(), True),StructField("GOTCharacter", StringType(), True)])
Sentimentrdd = LikedCharacters.map(lambda x: (x[0],' '.join(re.sub(regex_str,"",x[1]).split()),x[2]))
Sentimentrdd = Sentimentrdd.map(lambda x: (x[0],TextBlob(x[1]).sentiment.polarity,x[2]))
dfsentiment = sqlContext.createDataFrame(Sentimentrdd,schema_Sentiment)
dfsentiment.registerTempTable("Sentiment")
SentimentDFToWrite = sqlContext.sql("SELECT Season AS GOTSeason,GOTCharacter,SUM(Polarity) AS OverallCount ,SUM(CASE WHEN Polarity = 0 THEN 1 ELSE 0 END) AS NeutralCount,SUM(CASE WHEN Polarity > 0 THEN 1 ELSE 0 END) AS PositiveCount,SUM(CASE WHEN Polarity < 0 THEN 1 ELSE 0 END) AS NegativeCount  FROM Sentiment  GROUP BY Season,GOTCharacter ")

SentimentDFToWrite.toPandas().to_csv('Sentiment.csv')
##Shocking Factor
ShockList = ["appal","dreadful","scandal", "outrageous","awful","disgust","deplor","wicked","omg","shock","surprise","amaze","jolt","astonish","disbelief","unbelei","blow","fright","stun","flabbergast","astound","astoni","trauma","surprise","devastate","alarm","shudder","tremor","suspense","thrill"]
shockingdf = sqlContext.sql(" SELECT concat(concat(Year(to_date(date)),(CASE WHEN length(concat(Month(to_date(date)),'')) >1 THEN Month(to_date(date)) ELSE  concat('0',Month(to_date(date))) END )),(CASE WHEN length(concat(dayofmonth(to_date(date)),'')) >1 THEN dayofmonth(to_date(date)) ELSE  concat('0',dayofmonth(to_date(date))) END )) as TweetDate,text  FROM tweets ")
ShockingFactorrdd = shockingdf.map(lambda x: (x[0],compare(x[1],ShockList))).filter(lambda p: (len(p[1]) >1))
schema_shocking = StructType([StructField("TweetDate", StringType(), True),StructField("ShockWord", StringType(), True)])
shockingdf = sqlContext.createDataFrame(ShockingFactorrdd,schema_shocking)
shockingdf.registerTempTable("ShockingFactor")
ShockingFactorDF = sqlContext.sql(" SELECT TweetDate,count(ShockWord) FROM ShockingFactor GROUP BY TweetDate  ")
#ShockingFactorDF = sqlContext.sql(" SELECT (CASE WHEN TweetDate <= '20110423' THEN 'S1-EPISODE1' WHEN TweetDate<= '20110430' THEN 'S1-EPISODE2' WHEN TweetDate<= '20110507' THEN 'S1-EPISODE3' WHEN TweetDate<= '20110514' THEN 'S1-EPISODE4' WHEN TweetDate<= '20110521' THEN 'S1-EPISODE5' WHEN TweetDate<= '20110528' THEN 'S1-EPISODE6' WHEN TweetDate<= '20110604' THEN 'S1-EPISODE7' WHEN TweetDate<= '20110612' THEN 'S1-EPISODE8' WHEN TweetDate<= '20110618' THEN 'S1-EPISODE9' WHEN TweetDate<= '20110630' THEN 'S1-EPISODE10' WHEN TweetDate<= '20120407' THEN 'S2-EPISODE1' WHEN TweetDate<= '20120414' THEN 'S2-EPISODE2'  WHEN TweetDate<= '20120421' THEN 'S2-EPISODE3' WHEN TweetDate<= '20120428' THEN 'S2-EPISODE4' WHEN TweetDate<= '20120505' THEN 'S2-EPISODE5' WHEN TweetDate<= '20120512' THEN 'S2-EPISODE6' WHEN TweetDate<= '20120519' THEN 'S2-EPISODE7' WHEN TweetDate<= '20120526' THEN 'S2-EPISODE8' WHEN TweetDate<= '20120602' THEN 'S2-EPISODE9' WHEN TweetDate<= '20120630' THEN 'S2-EPISODE10' WHEN TweetDate<= '20130407' THEN 'S3-EPISODE1' WHEN TweetDate<= '20130414' THEN 'S3-EPISODE2' WHEN TweetDate<= '20130421' THEN 'S3-EPISODE3' WHEN TweetDate<= '20130428' THEN 'S3-EPISODE4' WHEN TweetDate<= '20130505' THEN 'S3-EPISODE5' WHEN TweetDate<= '20130512' THEN 'S3-EPISODE6' WHEN TweetDate<= '20130519' THEN 'S3-EPISODE7' WHEN TweetDate<= '20130526' THEN 'S3-EPISODE8' WHEN TweetDate<= '20130602' THEN 'S3-EPISODE9' WHEN TweetDate<= '20130630' THEN 'S3-EPISODE10' WHEN TweetDate<= '20140412' THEN 'S4-EPISODE1' WHEN TweetDate<= '20140419' THEN 'S4-EPISODE2' WHEN TweetDate<= '20140426' THEN 'S4-EPISODE3' WHEN TweetDate<= '20140503' THEN 'S4-EPISODE4' WHEN TweetDate<= '20140510' THEN 'S4-EPISODE5' WHEN TweetDate<= '20140517' THEN 'S4-EPISODE6' WHEN TweetDate<= '20140531' THEN 'S4-EPISODE7' WHEN TweetDate<= '20140607' THEN 'S4-EPISODE8' WHEN TweetDate<= '20140614' THEN 'S4-EPISODE9' WHEN TweetDate<= '20140630' THEN 'S4-EPISODE10' WHEN TweetDate<= '20150418' THEN 'S5-EPISODE1' WHEN TweetDate<= '20150425' THEN 'S5-EPISODE2' WHEN TweetDate<= '20150502' THEN 'S5-EPISODE3' WHEN TweetDate<= '20150509' THEN 'S5-EPISODE4' WHEN TweetDate<= '20150516' THEN 'S5-EPISODE5' WHEN TweetDate<= '20150523' THEN 'S5-EPISODE6' WHEN TweetDate<= '20150530' THEN 'S5-EPISODE7' WHEN TweetDate<= '20150606' THEN 'S5-EPISODE8' WHEN TweetDate<= '20150613' THEN 'S5-EPISODE9' WHEN TweetDate<= '20150630' THEN 'S5-EPISODE10' WHEN TweetDate<= '20160430' THEN 'S6-EPISODE1' WHEN TweetDate<= '20160507' THEN 'S6-EPISODE2' WHEN TweetDate<= '20160514' THEN 'S6-EPISODE3' WHEN TweetDate<= '20160521' THEN 'S6-EPISODE4' WHEN TweetDate<= '20160528' THEN 'S6-EPISODE5' WHEN TweetDate<= '20160604' THEN 'S6-EPISODE6' WHEN TweetDate<= '20160611' THEN 'S6-EPISODE7' WHEN TweetDate<= '20160618' THEN 'S6-EPISODE8' WHEN TweetDate<= '20160625' THEN 'S6-EPISODE9' WHEN TweetDate<= '20160730' THEN 'S6-EPISODE10' END) AS EpisodeInfo,count(ShockWord) AS ShockingCount FROM ShockingFactor GROUP BY (CASE WHEN TweetDate<= '20110423' THEN 'S1-EPISODE1' WHEN TweetDate<= '20110430' THEN 'S1-EPISODE2' WHEN TweetDate<= '20110507' THEN 'S1-EPISODE3' WHEN TweetDate<= '20110514' THEN 'S1-EPISODE4' WHEN TweetDate<= '20110521' THEN 'S1-EPISODE5' WHEN TweetDate<= '20110528' THEN 'S1-EPISODE6' WHEN TweetDate<= '20110604' THEN 'S1-EPISODE7' WHEN TweetDate<= '20110612' THEN 'S1-EPISODE8' WHEN TweetDate<= '20110618' THEN 'S1-EPISODE9' WHEN TweetDate<= '20110630' THEN 'S1-EPISODE10' WHEN TweetDate<= '20120407' THEN 'S2-EPISODE1' WHEN TweetDate<= '20120414' THEN 'S2-EPISODE2'  WHEN TweetDate<= '20120421' THEN 'S2-EPISODE3' WHEN TweetDate<= '20120428' THEN 'S2-EPISODE4' WHEN TweetDate<= '20120505' THEN 'S2-EPISODE5' WHEN TweetDate<= '20120512' THEN 'S2-EPISODE6' WHEN TweetDate<= '20120519' THEN 'S2-EPISODE7' WHEN TweetDate<= '20120526' THEN 'S2-EPISODE8' WHEN TweetDate<= '20120602' THEN 'S2-EPISODE9' WHEN TweetDate<= '20120630' THEN 'S2-EPISODE10' WHEN TweetDate<= '20130407' THEN 'S3-EPISODE1' WHEN TweetDate<= '20130414' THEN 'S3-EPISODE2' WHEN TweetDate<= '20130421' THEN 'S3-EPISODE3' WHEN TweetDate<= '20130428' THEN 'S3-EPISODE4' WHEN TweetDate<= '20130505' THEN 'S3-EPISODE5' WHEN TweetDate<= '20130512' THEN 'S3-EPISODE6' WHEN TweetDate<= '20130519' THEN 'S3-EPISODE7' WHEN TweetDate<= '20130526' THEN 'S3-EPISODE8' WHEN TweetDate<= '20130602' THEN 'S3-EPISODE9' WHEN TweetDate<= '20130630' THEN 'S3-EPISODE10' WHEN TweetDate<= '20140412' THEN 'S4-EPISODE1' WHEN TweetDate<= '20140419' THEN 'S4-EPISODE2' WHEN TweetDate<= '20140426' THEN 'S4-EPISODE3' WHEN TweetDate<= '20140503' THEN 'S4-EPISODE4' WHEN TweetDate<= '20140510' THEN 'S4-EPISODE5' WHEN TweetDate<= '20140517' THEN 'S4-EPISODE6' WHEN TweetDate<= '20140531' THEN 'S4-EPISODE7' WHEN TweetDate<= '20140607' THEN 'S4-EPISODE8' WHEN TweetDate<= '20140614' THEN 'S4-EPISODE9' WHEN TweetDate<= '20140630' THEN 'S4-EPISODE10' WHEN TweetDate<= '20150418' THEN 'S5-EPISODE1' WHEN TweetDate<= '20150425' THEN 'S5-EPISODE2' WHEN TweetDate<= '20150502' THEN 'S5-EPISODE3' WHEN TweetDate<= '20150509' THEN 'S5-EPISODE4' WHEN TweetDate<= '20150516' THEN 'S5-EPISODE5' WHEN TweetDate<= '20150523' THEN 'S5-EPISODE6' WHEN TweetDate<= '20150530' THEN 'S5-EPISODE7' WHEN TweetDate<= '20150606' THEN 'S5-EPISODE8' WHEN TweetDate<= '20150613' THEN 'S5-EPISODE9' WHEN TweetDate<= '20150630' THEN 'S5-EPISODE10' WHEN TweetDate<= '20160430' THEN 'S6-EPISODE1' WHEN TweetDate<= '20160507' THEN 'S6-EPISODE2' WHEN TweetDate<= '20160514' THEN 'S6-EPISODE3' WHEN TweetDate<= '20160521' THEN 'S6-EPISODE4' WHEN TweetDate<= '20160528' THEN 'S6-EPISODE5' WHEN TweetDate<= '20160604' THEN 'S6-EPISODE6' WHEN TweetDate<= '20160611' THEN 'S6-EPISODE7' WHEN TweetDate<= '20160618' THEN 'S6-EPISODE8' WHEN TweetDate<= '20160625' THEN 'S6-EPISODE9' WHEN TweetDate<= '20160730' THEN 'S6-EPISODE10' END) ")
ShockingFactorDF.toPandas().to_csv('ShockingFactor.csv')
