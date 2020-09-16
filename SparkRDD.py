"""TASK 1: STEP 1"""

from pyspark.sql import functions as func
import pyspark.sql.functions as f
from pyspark.sql import *
from pyspark.sql.functions import col
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import desc
from pyspark import SparkContext
sc =SparkContext.getOrCreate()
#READING THE SOURCE FILE
df = spark.read.format('com.databricks.spark.csv').\
                               options(header='true', \
                               inferschema='true').\
                load("Amazon_Responded_Oct05.csv",header=True)

df=df.select('id_str', 'tweet_created_at', 'user_verified', 'favorite_count', 'retweet_count', 'text_')                

#FILTERING ONLY VERIFIED USERS
filterddf=df.filter(df.user_verified == "True")

#DISPLAYING THE COUNT OF VERIFIED USERS
filterddf.count()

"""COUNT OF VERIFIED USERS:171797

TASK 1:STEP 2
"""

#SEPERATING DATE OUT OF THE TIMESTAMP COLUMN
filterddf_new=filterddf.withColumn('date_tweet_created_at', df.tweet_created_at.substr(5, 6))

#GROUPING BY THE DATE AND DISPLAYING THE TOP DATE WITH MAXIMUM TWEETS
countDistinctDF=filterddf_new.groupBy(['date_tweet_created_at']).count().orderBy(desc("count")).show(1)

"""DATE WITH HIGHEST TWEETS:JAN 03

TASK 1:STEP 3
"""

#FILTERING RECORDS FOR DATE FOUND IN PREVIOUS STEP
filterddf_new_3=filterddf_new.filter(filterddf_new.date_tweet_created_at == "Jan 03")

#CALCULATING SUM OF "favorite_count" and "retweet count" AND DISPLAYING THE TOP 100 TWEETS WITH THE HIGHEST SUM
final_text=filterddf_new_3.withColumn('sumoftweets', filterddf_new_3.favorite_count+filterddf_new_3.retweet_count).orderBy(desc("sumoftweets")).limit(100).select("text_")
final_text.show()

#WORD COUNT OF THE TWEETS
df_final = final_text.withColumn('Word', f.explode(f.split(f.lower(f.col('text_')), ' ')))\
    .groupBy('Word')\
    .count()\
    .sort('count', ascending=False)
df_final.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("/content/Wordcount.csv")

"""TASK 2:"""

#READING THE FILE find_test.csv
findtext=spark.read.csv('/content/find_text.csv',header=True)

#JOINING THE TWO FILES USING id_str
result=findtext.join(df, "id_str", "LeftOuter").select("id_str","text_")

#WRITING THE RESULT TO A FILE
result.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("/content/Join_result.csv")