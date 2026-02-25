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

df = spark.sql("SELECT * FROM SharePoint_LakeHouse.dbo.Dirty_Data ")
display(df)

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

df.withColumn('bracket2' ,regexp_replace(col("col_brackets"), r"(\D+)", "")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn('col_pat', regexp_replace(col('col_paren'),r'(\D+)',"")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn('names', regexp_replace(col('col_name'),r'[^a-zA-Z/s]+',"")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

col_in = F.col("col_date")  # replace with your column name

# 1) Clean: trim, remove commas
cleaned = F.trim(F.regexp_replace(col_in, ",", ""))

# 2) List of patterns we expect in your data
patterns = [
    "d/M/yyyy",     # 24/12/2024
    "d-M-yyyy",     # 24-12-2024
    "d MMM yyyy",   # 9 Oct 2023
    "d MMMM yyyy",  # 9 October 2023
    "MMM d yyyy",   # Feb 24 2024  (commas removed in cleaning)
    "MMMM d yyyy",  # February 24 2024
    "yyyy-M-d",
    "yyyy-MM-dd"     # 2024-12-24
    "M/d/yyyy",     # 12/24/2024
    "M-d-yyyy",     # 12-24-2024
    "M/yyyy/d",     # 02/2020/29  (MM/YYYY/DD)
    "M-yyyy-d",     # 02-2020-29
]

# 3) Try parsing with each pattern, then coalesce
parsed_candidates = [F.to_date(cleaned, p) for p in patterns]
parsed = F.coalesce(*parsed_candidates)

# 4) Format as DD/MM/YYYY (string)
df_out = df.withColumn("date_ddmmyyyy", F.date_format(parsed, "dd/MM/yyyy"))

df_out.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn('col_n2', regexp_replace(col('col_number'),r'[^0-9.]+',"")).show()

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
