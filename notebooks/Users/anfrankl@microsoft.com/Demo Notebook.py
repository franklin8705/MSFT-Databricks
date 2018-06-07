# Databricks notebook source
print("Spark Session: ",spark,'\n',
      "SQL Context: ", sqlContext,'\n',
     "DBFS Utilities: ", dbutils, '\n',
     "Spark Context",sc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Shell Commands to create the directory and download the churn data from SGI website
# MAGIC * Only run once

# COMMAND ----------

# MAGIC %sh
# MAGIC #Local File Storage
# MAGIC mkdir /tmp/churn
# MAGIC wget https://community.watsonanalytics.com/wp-content/uploads/2015/03/WA_Fn-UseC_-Telco-Customer-Churn.csv -O /tmp/churn/churn.csv 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Current Working Directory

# COMMAND ----------

pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l

# COMMAND ----------

who

# COMMAND ----------

# MAGIC %md
# MAGIC #### Change Directory

# COMMAND ----------

cd /tmp/churn

# COMMAND ----------

pwd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Make sure data exists in the folder

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the Data
# MAGIC - This makes it easy to reference

# COMMAND ----------

# MAGIC %py
# MAGIC #Create folder in DBFS (Pointer to the specified file)
# MAGIC dbutils.fs.mkdirs("/mnt/churn")
# MAGIC dbutils.fs.mv("file:///tmp/churn/churn.csv","/mnt/churn/churn.data")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Modules Needed

# COMMAND ----------

from pyspark.sql.types import * 
#StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the schema
# MAGIC * What columns are strings
# MAGIC * What columns are integers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create initial dataframes

# COMMAND ----------

df = (spark.read.option("delimiter",",").option("inferSchema","true").option("header","true").csv("dbfs:/mnt/churn/churn.data"))

# COMMAND ----------

#Import Option 2
#sparkdf = sqlContext.read.format('csv').load('/mnt/churn/churn.data')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display Method in Databricks

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple Tasks
# MAGIC - Count Cases
# MAGIC - Count Churned Cases
# MAGIC - Count Unchurned

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Python

# COMMAND ----------

#What is df?
print(type(df) )

#Convert to Pandas DF
df_py=df.toPandas()

#Use Pandas print method
df_py.head()

# COMMAND ----------

#Count observations
df_py.shape[0]

# COMMAND ----------

#Count Cases
#df_py[df_py['Churn']=='Yes'].shape
df_py['Churn'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC #### R

# COMMAND ----------

# MAGIC %r
# MAGIC #Import SparkR package
# MAGIC library(SparkR)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check what files are available

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/churn/

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC #Notice how we R magic is invoked to import the data as a "SparkR" Dataframe
# MAGIC df_r<-read.df("dbfs:/mnt/churn/churn.data",source="csv",header="true",inferSchema="true")

# COMMAND ----------

# MAGIC %r
# MAGIC typeof(df_r)

# COMMAND ----------

# MAGIC %r
# MAGIC df_rl<-collect(df_r)

# COMMAND ----------

# MAGIC %r
# MAGIC head(df_rl)

# COMMAND ----------

# MAGIC %r
# MAGIC #Total number of observations
# MAGIC dim(df_rl)

# COMMAND ----------

# MAGIC %r
# MAGIC #How many churn cases
# MAGIC sum(df_rl$Churn=='Yes')
# MAGIC 
# MAGIC #note: 
# MAGIC #sum(df_r$Churn=='Yes')  #does not yield correct results

# COMMAND ----------

# MAGIC %r
# MAGIC temp_churn<-as.numeric(df_rl$Churn=="Yes")

# COMMAND ----------

# MAGIC %r
# MAGIC r<-hist(temp_churn)
# MAGIC r$counts

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC #How many churned cases and unchurned
# MAGIC table(df_rl$Churn)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pyspark

# COMMAND ----------

numCases=df.count()
numChurned=df.filter(col('Churn')=='Yes').count()
print(numCases,numChurned,(numCases-numChurned))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create New Optimized Data Lake for Analytics

# COMMAND ----------

df.repartition(1).write.parquet('/mnt/data-lake/demo-data/churndata')

# COMMAND ----------

