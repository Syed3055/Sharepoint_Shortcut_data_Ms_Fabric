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
import re


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SharePoint_LakeHouse.dbo.Dirty_Data")
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

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('col_brackets2', regexp_replace(col('col_brackets'),r'[^0-9]',""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



df.withColumn('col1', regexp_extract(col('col_brackets'),r'(\d+)',1)).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn('text2', regexp_replace(col("col_name"), r"[^A-Za-z\s]", ""))\
  .withColumn('text3', regexp_replace(col("text2")," ","")).show()

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
