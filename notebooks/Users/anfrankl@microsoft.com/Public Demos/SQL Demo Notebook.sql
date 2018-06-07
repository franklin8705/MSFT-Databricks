-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## This notebook is to show Spark SQL functionality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Get the churn dataset
-- MAGIC - We have already mounted this directory, so no need to do it again. 

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /mnt/churn/

-- COMMAND ----------

# This is what the code looks like for data in blob storage:

# dbutils.fs.mount(
#   source = "wasbs://source@adbworkshops.blob.core.windows.net/",
#   mount_point = "/mnt/training-sources/",
#   extra_configs = {"fs.azure.sas.source.adbworkshops.blob.core.windows.net": "SAS-KEY"})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Spark SQL operates on "Tables"
-- MAGIC - How to create a "Table"?
-- MAGIC 
-- MAGIC (1) UI
-- MAGIC 
-- MAGIC (2) Programmatically

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Explore the UI
-- MAGIC - Select the "Data" Dial on left panel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Explore Programmatically

-- COMMAND ----------

display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Call python magic
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %r
-- MAGIC #Call R magic
-- MAGIC head(df_rl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Why are these dataframes NOT defined?
-- MAGIC - Notice Spark Session object

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print("Spark Session: ",spark,'\n',
-- MAGIC       "SQL Context: ", sqlContext,'\n',
-- MAGIC      "DBFS Utilities: ", dbutils, '\n',
-- MAGIC      "Spark Context",sc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL Tasks
-- MAGIC - Create a table
-- MAGIC - Observe (read) the table
-- MAGIC - Observe payment types for churned accounts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create temp table from dataframe in DBFS

-- COMMAND ----------

CREATE TEMPORARY TABLE temp_churn_table
USING parquet
OPTIONS (
  path '/mnt/data-lake/demo-data/churndata')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Read the table

-- COMMAND ----------

SELECT * FROM temp_churn_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Payment Types for Churned

-- COMMAND ----------

SELECT PaymentMethod,count(*) as payment_churn 
FROM temp_churn_table
WHERE Churn='Yes'
GROUP BY PaymentMethod

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Temp Table 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dataFrame='dbfs:/mnt/churn/churn.data'
-- MAGIC df_py = spark.read.format('csv').option('header','true').option('inferSchema','true').load(dataFrame)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # create local table from dataframe
-- MAGIC df_py.createOrReplaceTempView('temp_churn_table_py')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Read Temp Table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC temp_churn_df_py=spark.sql('SELECT * FROM temp_churn_table_py')
-- MAGIC display(temp_churn_df_py)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Payment Types for Churned

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display( (spark.sql("SELECT PaymentMethod,count(*) as payment_churn FROM temp_churn_table WHERE Churn='Yes' GROUP BY PaymentMethod"))
-- MAGIC        )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### R

-- COMMAND ----------

-- MAGIC %r
-- MAGIC library(SparkR)
-- MAGIC #Notice how we R magic is invoked to import the data as a "Spark" Dataframe in R context
-- MAGIC df_r<-read.df("dbfs:/mnt/churn/churn.data",source="csv",header="true",inferSchema="true")

-- COMMAND ----------

-- MAGIC %r
-- MAGIC registerTempTable(df_r,'temp_churn_table_r')

-- COMMAND ----------

-- MAGIC %r
-- MAGIC temp_churn_df_r <-sql('SELECT * FROM temp_churn_table_r')

-- COMMAND ----------

-- MAGIC %r
-- MAGIC display(temp_churn_df_r)

-- COMMAND ----------

