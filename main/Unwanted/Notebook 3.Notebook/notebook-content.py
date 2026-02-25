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

df = spark.sql("SELECT * FROM SharePoint_LakeHouse.dbo.Dirty_Data ")
display(df)

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

df.select([count(when(col(c).isNull(), c)).alias(c)for c in df.columns]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('clean_brackets', regexp_replace(col('col_brackets'),r'[^0-9]',""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('clean_paren', regexp_replace(col('col_paren'),r'[^0-9]',""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('clean_name', regexp_replace(col('col_name'),r'[^a-zA-Z/s]',""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('clean_mix', regexp_replace(col('col_mix'),r'[^0-9]',""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('clean_number', regexp_replace(col('col_number'),r'[^0-9+.]',""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(col('col_date')).distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# If your DataFrame is already loaded, skip this sample input:
# sample = [("Apr 22, 2024",),("01/2025/12",),("2024/12/24",),("01/02/2025",),
#           ("19 Nov 2026",),("24/12/2024",),("9 Apr 2023",),("Feb 5, 2022",),
#           ("Jan 24, 2024",),("10 Nov 2025",),("24/2024/12",),("2024/01/22",),
#           ("5 May 2022",),("31 Jun 2024",),("2025-12-10",),("10-12-2025",),
#           ("22/2024/01",),("19 Dec 2026",),("19 Apr 2026",),("05-11-2022",)]
# df = spark.createDataFrame(sample, ["col_date"])

# 1) Normalize text a bit (trim, collapse spaces, month casing)
df1 = (
    df
    .withColumn("s", F.trim(F.col("col_date")))
    .withColumn("s", F.regexp_replace("s", r"\s+", " "))
    .withColumn("s", F.initcap("s"))  # e.g., 'jan'/'JAN' -> 'Jan'
    .withColumn("s_no_comma", F.regexp_replace("s", ",", ""))  # for "Jan 24 2024"
    # Fix odd dd/YYYY/MM into dd/MM/YYYY (e.g., 01/2025/12 -> 01/12/2025)
    .withColumn(
        "s_swapped",
        F.when(
            F.col("s").rlike(r"^\d{1,2}/\d{4}/\d{1,2}$"),
            F.regexp_replace("s", r"^(\d{1,2})/(\d{4})/(\d{1,2})$", r"$1/$3/$2")
        ).otherwise(F.col("s"))
    )
)

# 2) Try multiple parse patterns (dd/MM/yyyy-first)
candidates = [
    # Worded month formats
    F.to_date("s", "MMM d, yyyy"),         # "Apr 22, 2024"
    F.to_date("s_no_comma", "d MMM yyyy"),  # "19 Nov 2026", "9 Apr 2023", "5 May 2022"
    F.to_date("s_no_comma", "MMM d yyyy"),  # "Jan 24 2024" (if comma missing)

    # Slash formats (prefer dd/MM/yyyy)
    F.to_date("s_swapped", "dd/MM/yyyy"),   # handles converted dd/YYYY/MM plus "24/12/2024"
    F.to_date("s_swapped", "d/M/yyyy"),     # single-digit day/month
    F.to_date("s", "yyyy/MM/dd"),           # "2024/12/24", "2024/01/22"

    # Dash formats
    F.to_date("s", "yyyy-MM-dd"),           # "2025-12-10"
    F.to_date("s", "dd-MM-yyyy"),           # "10-12-2025", "05-11-2022"
    F.to_date("s", "d-M-yyyy"),             # single-digit variants
]

df_out = (
    df1
    .withColumn("parsed_date", F.coalesce(*candidates))
    .withColumn("date_ddMMyyyy", F.date_format("parsed_date", "dd/MM/yyyy"))  # NULL if parsed_date is NULL
    .select("col_date", "date_ddMMyyyy")  # keep original + final
)

# Show the result (invalids like "31 Jun 2024" will be NULL)
df_out.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_out = df_out.withColumn("s", F.regexp_replace("date_ddMMyyyy", r"[^A-Za-z0-9/\\-, ]", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_out.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_out = (
    df
    .withColumn("s", F.trim(F.col("col_date")))
    .withColumn("s", F.regexp_replace("s", r"\s+", " "))  # collapse spaces
    .withColumn(
        "parsed_date",
        F.coalesce(
            F.to_date(F.col("s"), "d/M/yyyy"),      # handles 23/7/2022
            F.to_date(F.col("s"), "dd/MM/yyyy"),    # handles 23/07/2022
            F.to_date(F.col("s"), "yyyy-MM-dd"),    # etc. add others as needed
            F.to_date(F.col("s"), "dd-MM-yyyy"),
            F.to_date(F.col("s"), "yyyy/MM/dd"),
            F.to_date(F.col("s"), "MMM d, yyyy"),
            F.to_date(F.col("s"), "d MMM yyyy"),
        )
    )
    .withColumn("date_ddMMyyyy", F.date_format(F.col("parsed_date"), "dd/MM/yyyy"))
)

# Rows that still don't match any pattern will have parsed_date = NULL,
# so date_ddMMyyyy will be NULL (i.e., blank/None) as you wanted.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
    "s",
    F.when(F.col("col_date").rlike(r"^\d{1,2}/\d{4}/\d{1,2}$"),
           F.regexp_replace("col_date", r"^(\d{1,2})/(\d{4})/(\d{1,2})$", r"$1/$3/$2"))
     .otherwise(F.col("col_date"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_out = df_out.withColumn('date_ddMMyyyy', regexp_replace(col('date_ddMMyyyy'),'23/7/2022',''))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_out.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_out.write.format('delta').mode('overwrite').save('Tables/dbo/clean_data1')

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
