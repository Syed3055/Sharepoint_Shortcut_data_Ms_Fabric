# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "423f6656-08bd-49b6-8e74-0872848ae4f7",
# META       "default_lakehouse_name": "SharePoint_LakeHouse",
# META       "default_lakehouse_workspace_id": "279d5d1f-ab4f-4c9b-929a-baffa3394de0",
# META       "known_lakehouses": [
# META         {
# META           "id": "423f6656-08bd-49b6-8e74-0872848ae4f7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SharePoint_LakeHouse.SharePoint_Data_Bronze.Sales_Data_Sharepoint ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.dtypes


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(mode(col('Country'))).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(count(when(col('Country').isNull(),1))).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mode_value = df.select(mode(col("Country")).alias('mode')).first()[0]
df = df.fillna({'Country':mode_value})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(col('Gender')).distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("Gender",col("Gender").replace({"M": "male", "MALE": "male", "f": "female", "F": "female", "female": "female"}, subset=["Gender"]))
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.replace(
    {"female":"female","f":"female","F":"female","M":"male","MALE":"male","FEMALE":'female','m':'male'},
    subset=["Gender"]
)
df.select(col('Gender')).distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

g_mode =  df.select(mode(col('Gender'))).first()[0]
display(g_mode)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gender_mode = df.select(mode(col('Gender')).alias('mode')).first()[0]
display(gender_mode)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= df.fillna({'Gender':g_mode})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select([count(when(col(c).isNull(),c)).alias(c) for c in df.columns]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

l_median = df.select(median(col('Last_Purchase_Date'))).first()[0]
display(l_median)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.window import *
w = Window.orderBy("Signup_Date").rowsBetween(Window.unboundedPreceding, 0)

df = df.withColumn(
    "Last_Purchase_Date",
    last("Last_Purchase_Date", ignorenulls=True).over(w)
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

f_median = df.select(median(col("Feedback_Score"))).alias('m').first()[0]
df = df.fillna({'Feedback_Score':f_median})
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select([count(when(col(c).isNull(),c)).alias(c)for c in df.columns]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df = df.na.drop(subset=['Customer_ID'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

m = df.select(median(col('Purchase_Amount')))
display(m)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
