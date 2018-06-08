# Databricks notebook source
# MAGIC %md
# MAGIC ## Perform predictive Modeling with Scikit-learn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Data into pandas dataframe
# MAGIC - Analysis performed on Drive Node (Not Distributed)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import to Spark DataFrame

# COMMAND ----------

# MAGIC %py
# MAGIC #Import Table to Spark Dataframe
# MAGIC dataset_churn_spark=spark.table('churn_ax')
# MAGIC 
# MAGIC #Cast TotalCharges into a DoubleType
# MAGIC from pyspark.sql.types import *
# MAGIC temp_churn_spark=dataset_churn_spark.withColumn("TotalCharges", dataset_churn_spark["TotalCharges"].cast(DoubleType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cast to Pandas DataFrame

# COMMAND ----------

import pandas as pd
churn_pd=temp_churn_spark.toPandas()

# COMMAND ----------

churn_pd.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### What are columns

# COMMAND ----------

churn_pd.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Isolate target

# COMMAND ----------

import numpy as np
churn_result=churn_pd['Churn']
y=np.where(churn_result=='Yes',1,0)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop some fields from analysis

# COMMAND ----------

to_drop=['customerID','Churn']
churn_feat_space=churn_pd.drop(to_drop,axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Separate Features
# MAGIC - Categorical
# MAGIC - Numerical

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Categorical & Numeric

# COMMAND ----------

churn_feat_space_cat=churn_feat_space.select_dtypes(include=['object'])
churn_feat_space_num=churn_feat_space.select_dtypes(include=[np.number])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Replace missing values

# COMMAND ----------

churn_feat_space_cat.fillna('U',inplace=True)
churn_feat_space_num.fillna(0,inplace=True)

# COMMAND ----------

print(churn_feat_space_cat.columns,'\n',
      'How many missing Cat values:',churn_feat_space_cat.isnull().sum().sum(),'\n',
      churn_feat_space_num.columns,'\n'
     'How many missing Num values:', churn_feat_space_num.isnull().sum().sum())

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Feature Scale and Dummify

# COMMAND ----------

from sklearn.preprocessing import StandardScaler, MinMaxScaler
churn_feat_space_cat_dummy=pd.get_dummies(churn_feat_space_cat,columns=churn_feat_space_cat.columns)

# COMMAND ----------

scaler=MinMaxScaler()
churn_feat_space_num_scaled=scaler.fit_transform(churn_feat_space_num)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Combine Imputed Cats and Nums

# COMMAND ----------

churn_features=pd.concat([pd.DataFrame(churn_feat_space_cat_dummy),pd.DataFrame(churn_feat_space_num_scaled,columns=churn_feat_space_num.columns)],axis=1)

# COMMAND ----------

churn_features.columns

# COMMAND ----------

X=churn_features

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross-Validation

# COMMAND ----------

from sklearn.cross_validation import KFold

def run_cv(X,y,clf_class,**kwargs):
    # Construct a kfolds object
    kf = KFold(len(y),n_folds=5,shuffle=True)
    y_pred = y.copy()

    # Iterate through folds
    for (train_index, test_index) in kf:
        X_train = X.as_matrix().astype(np.float)[train_index] 
        X_test = X.as_matrix().astype(np.float)[list(test_index)]
        y_train = y[train_index]
        # Initialize a classifier with key word arguments
        clf = clf_class(**kwargs)
        clf.fit(X_train,y_train)
        y_pred[test_index] = clf.predict(X_test)
    return y_pred

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Modeling Tournament
# MAGIC - Support Vector Machines
# MAGIC - Random Forest
# MAGIC - K-Nearest Neighbors Classifier

# COMMAND ----------

from sklearn.tree import DecisionTreeClassifier as DTC
from sklearn.ensemble import RandomForestClassifier as RF
from sklearn.neighbors import KNeighborsClassifier as KNN

def accuracy(y_true,y_pred):
    # NumPy interprets True and False as 1. and 0.
    return np.mean(y_true == y_pred)

# COMMAND ----------

print("Support vector machines:")
print(accuracy(y, run_cv(X,y,DTC)) )
print("Random forest:")
print(accuracy(y, run_cv(X,y,RF)))
print("K-nearest-neighbors:")
print("%.3f" % accuracy(y, run_cv(X,y,KNN)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Confusion Matrix

# COMMAND ----------

from sklearn.metrics import confusion_matrix

y = np.array(y)
class_names = np.unique(y)

confusion_matrices = [
    ( "Decision Tree", confusion_matrix(y,run_cv(X,y,DTC)) ),
    ( "Random Forest", confusion_matrix(y,run_cv(X,y,RF)) ),
    ( "K-Nearest-Neighbors", confusion_matrix(y,run_cv(X,y,KNN)) ),
]


# COMMAND ----------

# MAGIC %md
# MAGIC #### The End

# COMMAND ----------

