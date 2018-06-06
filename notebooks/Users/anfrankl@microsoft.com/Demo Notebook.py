# Databricks notebook source
print("Spark Context: ",spark,'\n',
      "SQL Context: ", sqlContext,'\n'
     "DBFS Utilities: ", dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Shell Commands to create the directory and download the churn data from SGI website
# MAGIC * Only run once

# COMMAND ----------

# MAGIC %sh
# MAGIC #Local File Storage
# MAGIC mkdir /tmp/churn
# MAGIC wget http://www.sgi.com/tech/mlc/db/churn.data -O /tmp/churn/churn.data 
# MAGIC wget http://www.sgi.com/tech/mlc/db/churn.test -O /tmp/churn/churn.test

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
# MAGIC dbutils.fs.mv("file:///tmp/churn/churn.data","/mnt/churn/churn.data")
# MAGIC dbutils.fs.mv("file:///tmp/churn/churn.test","/mnt/churn/churn.test")

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

churn_schema =StructType([
  StructField("state",StringType(), False),
  StructField("account_length",DoubleType(), False),    
  StructField("area_code",DoubleType(), False),    
  StructField("phone_number",StringType(), False), 
  StructField("international_plan",StringType(), False),  
  StructField("voice_mail_plan",StringType(), False),    
  StructField("number_vmail_messages",DoubleType(), False),
  StructField("total_day_minutes",DoubleType(), False),    
  StructField("total_day_calls",DoubleType(), False),    
  StructField("total_day_charge",DoubleType(), False),   
  StructField("total_eve_minutes",DoubleType(), False),   
  StructField("total_eve_calls",DoubleType(), False),    
  StructField("total_eve_charge",DoubleType(), False),   
  StructField("total_night_minutes",DoubleType(), False), 
  StructField("total_night_calls",DoubleType(), False),   
  StructField("total_night_charge",DoubleType(), False),  
  StructField("total_intl_minutes",DoubleType(), False),  
  StructField("total_intl_calls",DoubleType(), False),    
  StructField("total_intl_charge",DoubleType(), False),   
  StructField("number_customer_service_calls",DoubleType(), False), 
  StructField("churned",StringType(), False) 
])


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create initial dataframes

# COMMAND ----------

df = spark.read.option("delimiter",",").option("inferSchema","true").schema(churn_schema).csv("dbfs:/mnt/churn/churn.data")


# COMMAND ----------

sparkdf = sqlContext.read.format('csv').load('/mnt/churn/churn.data')

# COMMAND ----------

display(sparkdf)

# COMMAND ----------

