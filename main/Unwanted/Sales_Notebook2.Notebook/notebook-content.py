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

#df.dtypes
df.columns

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

df = df.withColumn('Gender', lower(trim(col('Gender'))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.replace({'m':'male','f':'female'},subset=['Gender'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

g_mo = df.select(mode(col('Gender'))).first()[0]
df =df.fillna({'Gender':g_mo})
df.select(col('Gender')).distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.groupBy('Gender').count().orderBy('count').show()

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

pi = df.groupby('Gender').pivot('Country').agg(round(sum(col('Purchase_Amount')),2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(pi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------- Imports ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W, types as T

spark = SparkSession.builder.getOrCreate()

# ---------- Sample DataFrames (SRC vs TGT) ----------
src_data = [
    # CustomerID, Name,   Country, Amount,  Tax
    (1, "Syed",  "India", 100.10,  5.005),
    (2, "Asha",  "USA",   200.00,  10.00),
    (3, "Raj",   "India", 300.00,  15.00),
    (4, "Meera", "UK",    150.00,  7.50),
    (5, "John",  "USA",   99.99,   5.00),
    (6, "Ana",   None,    50.00,   None),
]
tgt_data = [
    (1, "Syed",  "India", 100.10,  5.005),      # same
    (2, "Asha",  "USA",   200.005, 10.00),      # small delta (within 0.01 tolerance? -> YES)
    (3, "raj ",  "India", 300.00,  15.00),      # casing + trailing space (normalized later)
    (4, "Meera", "UK",    150.50,  7.50),       # 0.50 delta (beyond 0.01 tolerance) -> change
    (5, "John",  "USA",   99.99,   5.00),       # same
    (6, "Ana",   None,    50.00,   None),       # same
    (7, "Mira",  "India", 120.00,  6.00),       # new row in TGT
]

df_src = spark.createDataFrame(src_data, ["CustomerID","Name","Country","Amount","Tax"])
df_tgt = spark.createDataFrame(tgt_data, ["CustomerID","Name","Country","Amount","Tax"])

# ---------- Reusable helper functions ----------

def compare_schema(df1, df2):
    """Return fields only in df1 and only in df2 as sets of tuples (name, type, nullable)."""
    s1 = {(f.name, f.dataType.simpleString(), f.nullable) for f in df1.schema.fields}
    s2 = {(f.name, f.dataType.simpleString(), f.nullable) for f in df2.schema.fields}
    return s1 - s2, s2 - s1

def null_counts(df):
    """Return a one-row DataFrame with null counts per column."""
    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    return df.agg(*exprs)

def normalize_strings(df, cols):
    """Trim and collapse internal whitespace (lowercase optional)."""
    out = df
    for c in cols:
        out = out.withColumn(c, F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))
        # Uncomment if you want case-insensitive equality:
        # out = out.withColumn(c, F.lower(F.col(c)))
    return out

def col_equal_with_tol(src_col, tgt_col, tol):
    """Return boolean Column: True if both null OR abs diff <= tol."""
    return (
        (F.col(src_col).isNull() & F.col(tgt_col).isNull()) |
        (F.abs(F.col(src_col).cast("double") - F.col(tgt_col).cast("double")) <= F.lit(tol))
    )

def duplicate_keys(df, keys):
    """Return rows where the business key is duplicated."""
    return df.groupBy(*keys).count().where(F.col("count") > 1)

def row_hash(df, cols):
    """
    Normalize nulls to a sentinel string, cast to string, concat with a delimiter,
    and produce a stable SHA-256 hash per row over 'cols'.
    """
    concat_cols = [F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in cols]
    return df.select(F.sha2(F.concat_ws("§", *concat_cols), 256).alias("row_hash"))

# ---------- Config ----------
keys = ["CustomerID"]
numeric_tolerances = {
    "Amount": 0.01,   # 1 cent tolerance
    "Tax":    0.001,  # 0.001 tolerance
}
string_cols = ["Name", "Country"]   # normalize whitespace; uncomment lower() above if you need case-insensitive compare

# ---------- 1) Quick sanity checks ----------
print("=== 1) QUICK SANITY CHECKS ===")
print("SRC schema:"); df_src.printSchema()
print("TGT schema:"); df_tgt.printSchema()

only_in_src, only_in_tgt = compare_schema(df_src, df_tgt)
print("Fields only in SRC:", only_in_src)
print("Fields only in TGT:", only_in_tgt)

src_count = df_src.count()
tgt_count = df_tgt.count()
print(f"Row counts -> SRC: {src_count}, TGT: {tgt_count}, Δ={tgt_count - src_count}")

print("Nulls in SRC:"); null_counts(df_src).show(truncate=False)
print("Nulls in TGT:"); null_counts(df_tgt).show(truncate=False)

# ---------- 2) Normalize (strings, numeric casts if needed) ----------
# (For this example we keep numeric types as-is and normalize Name/Country whitespace.)
df_src_n = normalize_strings(df_src, string_cols)
df_tgt_n = normalize_strings(df_tgt, string_cols)

# ---------- 3) Keyed comparison ----------
print("=== 3) KEYED COMPARISON ===")
# 3a) Missing keys on either side
src_keys = df_src_n.select(*keys).dropDuplicates()
tgt_keys = df_tgt_n.select(*keys).dropDuplicates()

missing_in_tgt = src_keys.join(tgt_keys, on=keys, how="left_anti")
missing_in_src = tgt_keys.join(src_keys, on=keys, how="left_anti")

print("Missing in TGT (present in SRC only):")
missing_in_tgt.show()  # Expect: none in this sample

print("Missing in SRC (present in TGT only):")
missing_in_src.show()  # Expect: CustomerID=7

# 3b) Compare values for matched keys (with numeric tolerance)
common_cols = sorted(set(df_src_n.columns).intersection(df_tgt_n.columns))
non_key_cols = [c for c in common_cols if c not in keys]

src_aligned = df_src_n.select(*keys, *[F.col(c).alias(f"{c}__src") for c in non_key_cols])
tgt_aligned = df_tgt_n.select(*keys, *[F.col(c).alias(f"{c}__tgt") for c in non_key_cols])

joined = src_aligned.join(tgt_aligned, on=keys, how="inner")

# Build diff flags per column (use tolerance for numeric cols)
diff_flags = []
for c in non_key_cols:
    src_c, tgt_c = f"{c}__src", f"{c}__tgt"
    if c in numeric_tolerances:
        diff_flags.append((~col_equal_with_tol(src_c, tgt_c, numeric_tolerances[c])).alias(f"{c}__diff"))
    else:
        diff_flags.append((
            F.coalesce(F.col(src_c).cast("string"), F.lit("__NULL__")) !=
            F.coalesce(F.col(tgt_c).cast("string"), F.lit("__NULL__"))
        ).alias(f"{c}__diff"))

diff_df = joined.select(
    *keys,
    *[F.col(f"{c}__src") for c in non_key_cols],
    *[F.col(f"{c}__tgt") for c in non_key_cols],
    *diff_flags
)

# Keep only rows where at least one column differs
any_diff = F.array_max(F.array(*[F.col(f"{c}__diff").cast("int") for c in non_key_cols])) == 1
differences = diff_df.where(any_diff)

print("Changed rows (keys matched, after tolerance):")
differences.show(truncate=False)
print(f"Changed rows count (matched keys): {differences.count()}")

# Optional: show which columns changed per row
changed_cols = F.array(*[F.when(F.col(f"{c}__diff"), F.lit(c)) for c in non_key_cols])
differences.withColumn("changed_columns", changed_cols).select(*keys, "changed_columns").show(truncate=False)

# ---------- 4) Numeric tolerance demo (what's being treated as equal?) ----------
print("=== 4) NUMERIC TOLERANCE CHECKS ===")
# Show a focused view of numeric columns with deltas


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src_data = [
    # CustomerID, Name,   Country, Amount,  Tax
    (1, "Syed",  "India", 100.10,  5.005),
    (2, "Asha",  "USA",   200.00,  10.00),
    (3, "Raj",   "India", 300.00,  15.00),
    (4, "Meera", "UK",    150.00,  7.50),
    (5, "John",  "USA",   99.99,   5.00),
    (6, "Ana",   None,    50.00,   None),
]
tgt_data = [
    (1, "Syed",  "India", 100.10,  5.005),      # same
    (2, "Asha",  "USA",   200.005, 10.00),      # small delta (within 0.01 tolerance? -> YES)
    (3, "raj ",  "India", 300.00,  15.00),      # casing + trailing space (normalized later)
    (4, "Meera", "UK",    150.50,  7.50),       # 0.50 delta (beyond 0.01 tolerance) -> change
    (5, "John",  "USA",   99.99,   5.00),       # same
    (6, "Ana",   None,    50.00,   None),       # same
    (7, "Mira",  "India", 120.00,  6.00),       # new row in TGT
]

df_src = spark.createDataFrame(src_data, ["CustomerID","Name","Country","Amount","Tax"])
df_tgt = spark.createDataFrame(tgt_data, ["CustomerID","Name","Country","Amount","Tax"])

# ---------- Reusable helper functions ----------

display(df_src)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_tgt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def compare_schema(df1, df2):
    """Return fields only in df1 and only in df2 as sets of tuples (name, type, nullable)."""
    s1 = {(f.name, f.dataType.simpleString(), f.nullable) for f in df1.schema.fields}
    s2 = {(f.name, f.dataType.simpleString(), f.nullable) for f in df2.schema.fields}
    return s1 - s2, s2 - s1

# ---------- 1) Quick sanity checks ----------
print("=== 1) QUICK SANITY CHECKS ===")
print("SRC schema:"); df_src.printSchema()
print("TGT schema:"); df_tgt.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

only_in_src, only_in_tgt = compare_schema(df_src, df_tgt)
print("Fields only in SRC:", only_in_src)
print("Fields only in TGT:", only_in_tgt)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W

spark = SparkSession.builder.getOrCreate()

data = [
    # cust_id, name,        city,         email,                      purchase_amount
    (101,     "Syed",       "Hyderabad",  "syed@example.com",         120.50),
    (101,     "Syed",       "Hyderabad",  "syed@example.com",         120.50),  # exact duplicate of row 1

    (102,     "Asha",       "Bengaluru",  "asha@example.com",         200.00),
    (103,     "Raj",        "Chennai",    "raj@example.com",          75.00),
    (103,     "Raj",        "Chennai",    "raj@example.com",          75.00),   # exact duplicate of row 4

    (104,     "Meera",      "Mumbai",     "meera@example.com",        150.00),
    (105,     "John",       "Pune",       "john@example.com",         99.99),

    # key-based dupes: same cust_id with slightly different attributes
    (106,     "Ana",        "Delhi",      "ana@example.com",          50.00),
    (106,     "Ana",        "New Delhi",  "ana@example.com",          50.00),   # duplicate on key (cust_id), values differ

    # another near-duplicate with casing/space differences
    (107,     "mira",       "Kolkata",    "mira@example.com",         120.00),
    (107,     "Mira ",      "Kolkata",    "mira@example.com",         120.00),  # duplicate on key; name spacing/case
]

df = spark.createDataFrame(
    data,
    ["cust_id", "name", "city", "email", "purchase_amount"]
)

df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

v = df.drop_duplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(v)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn('name',initcap(trim(col('name')))).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

win = Window.partitionBy(col('city')).orderBy(col('city'))
df=df.withColumn('rn',row_number().over(win))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.filter(col('rn')!=2).show()

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
