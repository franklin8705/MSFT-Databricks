# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Shell Commands to create the directory and download the churn data from SGI website
# MAGIC * Only run once

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /tmp/churn
# MAGIC wget http://www.sgi.com/tech/mlc/db/churn.data -O /tmp/churn/churn.data 
# MAGIC wget http://www.sgi.com/tech/mlc/db/churn.test -O /tmp/churn/churn.test

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Try a Unix command without the utility

# COMMAND ----------

pwd

# COMMAND ----------

# MAGIC %git 
# MAGIC status

# COMMAND ----------

who

# COMMAND ----------

cd /tmp/churn

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
# MAGIC import numpy as np

# COMMAND ----------

np.sum(1+1)

# COMMAND ----------

# MAGIC %r
# MAGIC a<-c(1,2,3,4,6)
# MAGIC sum(a)

# COMMAND ----------

b<-c(1,2,3)
sum(b)

# COMMAND ----------

