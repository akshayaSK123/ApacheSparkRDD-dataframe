"""STEP 1

Creating the dataframe with necessary columns
"""

from pyspark.sql import functions as func
import pyspark.sql.functions as f
from pyspark.sql import *
from pyspark.sql.functions import col
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import desc
from pyspark import SparkContext
from pyspark.sql.functions import col, lit
from functools import reduce

sc =SparkContext.getOrCreate()
#READING THE SOURCE FILE
df = spark.read.format('com.databricks.spark.csv').\
                               options(header='true', \
                               inferschema='true').\
                load("Amazon_Responded_Oct05.csv",header=True)

df_select=df.select('tweet_created_at', 'user_verified', 'user_screen_name','User_id_str')                
df_select.count()
element_list = ["'7","'8"]
starts_with = reduce(
    lambda x, y: x | y,
    [col("id_str").startswith(s) for s in element_list], 
    lit(False))
df_clean=df_select.where(starts_with)

"""Find out the users who are active in at least five Days and storing it in a Dataframe named dailyactiveusers"""

from pyspark.sql.functions import *
from pyspark.sql.functions import concat, col, lit
#SEPERATING DATE OUT OF THE TIMESTAMP COLUMN
filterddf_new1=df_clean.withColumn('date_tweet_created_at',concat(df.tweet_created_at.substr(5, 6),lit(" "),df.tweet_created_at.substr(27, 4)))
filterddf_new1.show(10)

from pyspark.sql.functions import *
#Extracting date value from Timestamp field
filterddf_new=df_clean.withColumn('date_tweet_created_at',concat(df.tweet_created_at.substr(5, 6),lit(" "),df.tweet_created_at.substr(27, 4)))

#Grouping by the date and user Screen Name and taking Users having Count>=5
countDistinctDF=filterddf_new.groupBy('date_tweet_created_at','User_id_str','user_screen_name').count().orderBy(desc("date_tweet_created_at"))
cou=countDistinctDF.groupBy('User_id_str','user_screen_name').count().alias('count').filter(column('count')>=5)

daily_active_users=cou.select('user_screen_name','User_id_str')
print("Count of daily active users : ",daily_active_users.count())

#Displaying the Daily_Active_Users Dataframe
daily_active_users.show(10)

"""STEP 2: 
Loading the Experiment.txt file and joining with Active users DF
"""

experiment_file1= spark.read.format('com.databricks.spark.csv').\
                               options(header='false', \
                               inferschema='true').\
                load("experiment.txt",header=False)
experiment_file=experiment_file1.withColumnRenamed("_c0","User_id_str")

#JOINING THE TWO FILES USING id_str
result = experiment_file.join(daily_active_users, on=['User_id_str'], how='left')
result.show(10)

#Keeping whether active column as 'yes' for users present in active user df wheras 'No' for Null values
dfnotnull_fina=result.withColumn("Whether_active",
       when(isnull(col("user_screen_name")) , "No")
       .otherwise("yes"))
      

experiment_user=dfnotnull_fina.select('User_id_str','Whether_active')
print("Count of experiment users : ",experiment_user.count())

experiment_user.show(10)

"""Calculating the percentage of Active users and Printing the Result"""

Activeusers=experiment_user.filter("Whether_active=='yes'")

#Number of Active users 
Activeusers.count()

#Total Nunmber of users in the Experiment file 
experiment_user.count()

#Identifying the % of Active users out of total users in Experiment file
print("Total percentage of active Users:",((Activeusers.count())/(experiment_user.count()))*100,"%")

#Totally 2.42 % of users are active users among the total user in Experiement List

"""STEP 3: 3 table join"""

#3 table join
import pyspark
conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
from pyspark.sql import SQLContext
sqlcontext = SQLContext(sc)

df = spark.read.format('com.databricks.spark.csv').\
                               options(header='true', \
                               inferschema='true').\
                load("Amazon_Responded_Oct05.csv",header=True)


#Reading all the data to be joined
df.registerTempTable("Amazonfulltable")
experiment_user.registerTempTable("Experimentuser")
daily_active_users.registerTempTable("dailyactiveuser")

#Join statement
finaldata = sqlcontext.sql("Select AM.* from Amazonfulltable AM join dailyactiveuser DAU on AM.user_id_str = DAU.user_id_str join Experimentuser EU on AM.user_id_str = EU.user_id_str")
print("Count after join : ",finaldata.count())

#Writing the result to target file
finaldata.toPandas().to_csv("Amazon_new.csv", sep=',')