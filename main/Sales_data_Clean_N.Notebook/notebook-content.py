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
from pyspark.sql.window import *


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

df.select([count(when(col(c).isNull(),c)).alias(c)for c in df.columns]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.dropna(subset=['Customer_ID'])

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

df = df.withColumn('Gender', initcap(trim(lower(col('Gender')))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.replace({'F':'Female','M':'Male'},subset=['Gender'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gender should fill null values with mode******

# CELL ********************

df = df.withColumn('Age', regexp_replace(col('Age'),r'[^0-9\-.]',""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.select(col('Age')).distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ge_me = df.select(median(col('Age'))).first()[0]
df = df.fillna({'Age':ge_me})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= df.withColumn('City', initcap(trim(lower(col('City')))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('Country', initcap(trim(lower(col('Country')))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.replace({'Ind':'India'}, subset=['Country'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gen_mode = df.select(mode(col('Gender'))).first()[0]
city_mode = df.select(mode(col('City'))).first()[0]
country_mode = df.select(mode(col('Country'))).first()[0]
df = df.fillna({'Gender':gen_mode})
df = df.fillna({'City':city_mode})
df = df.fillna({'Country':country_mode})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pur_median = df.select(median(col('Purchase_Amount'))).first()[0]
df = df.fillna({'Purchase_Amount':pur_median})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

last_ffill = Window.orderBy('Signup_Date').rowsBetween(Window.unboundedPreceding,0)
df = df.withColumn('Last_Purchase_Date',last('Last_Purchase_Date', ignorenulls=True).over(last_ffill))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

feed_mode = df.select(mode(col('Feedback_Score'))).first()[0]
df = df.fillna({'Feedback_Score':feed_mode})

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

df = df.dropDuplicates(["Customer_ID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format('delta').mode('overwrite').save('Tables/SharePoint_Data_Silver/Sales_sil')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
