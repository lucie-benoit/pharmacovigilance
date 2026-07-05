"""
faers_schema.py
================
Shared schema definition and column mapping for the FAERS dataset.

It defines:
  - raw_schema : the StructType matching the raw FAERS CSV (30 columns)
  - map_columns(df_raw) : a function that derives/renames the columns our
    analyses actually need, from the raw FAERS fields.

Both pharma_batch.py and pharma_streaming.py import this module so the
schema and derivation logic only ever live in one place.
"""

from pyspark.sql.types import *
from pyspark.sql.functions import col, when, lit

# ================ RAW FAERS SCHEMA ==================================
# Matches the columns as delivered by the FAERS dataset CSV.

raw_schema = StructType([
    StructField("report_id", StringType(), True),
    StructField("receive_date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("quarter", StringType(), True),
    StructField("serious", StringType(), True),
    StructField("serious_flags", StringType(), True),
    StructField("is_fatal", BooleanType(), True),
    StructField("is_hospitalized", BooleanType(), True),
    StructField("is_life_threat", BooleanType(), True),
    StructField("is_disabling", BooleanType(), True),
    StructField("reactions", StringType(), True),
    StructField("primary_reaction", StringType(), True),
    StructField("reaction_outcomes", StringType(), True),
    StructField("patient_recovered", BooleanType(), True),
    StructField("num_reactions", IntegerType(), True),
    StructField("suspect_drug", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("drug_route", StringType(), True),
    StructField("drug_indication", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("pharm_class", StringType(), True),
    StructField("num_drugs", IntegerType(), True),
    StructField("drug_count_category", StringType(), True),
    StructField("patient_age_years", DoubleType(), True),
    StructField("age_group", StringType(), True),
    StructField("patient_sex", StringType(), True),
    StructField("patient_weight_kg", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("report_age_days", IntegerType(), True),
])


# ================ COLUMN MAPPING / DERIVATION ========================
#   report_date        <- receive_date
#   drug_name           <- suspect_drug (generic name)
#   drug_class          <- pharm_class
#   adverse_event       <- primary_reaction
#   severity            <- "Severe" if serious == "Yes" else "Non-Severe"
#   outcome             <- "Recovered" if patient_recovered else "Not Recovered"
#   seriousness_score   <- is_fatal*4 + is_life_threat*3 + is_hospitalized*2 + is_disabling*1
#   age_group, country, manufacturer -> used as-is

def map_columns(df_raw):
    """Takes a DataFrame matching raw_schema (batch or streaming) and
    returns a DataFrame with the columns used by our pharmacovigilance
    analyses. Works identically on static and streaming DataFrames since
    it only uses column-level transformations (no aggregation, no
    action like count() is triggered here)."""
    return df_raw.select(
        col("report_id"),
        col("receive_date").alias("report_date"),
        col("suspect_drug").alias("drug_name"),
        col("pharm_class").alias("drug_class"),
        col("primary_reaction").alias("adverse_event"),
        when(col("serious") == "Yes", lit("Severe")).otherwise(lit("Non-Severe")).alias("severity"),
        when(col("patient_recovered") == True, lit("Recovered")).otherwise(lit("Not Recovered")).alias("outcome"),
        col("age_group"),
        col("country"),
        col("manufacturer"),
        col("is_fatal"),          
        col("is_hospitalized"),   
        (
            when(col("is_fatal") == True, lit(4)).otherwise(lit(0)) +
            when(col("is_life_threat") == True, lit(3)).otherwise(lit(0)) +
            when(col("is_hospitalized") == True, lit(2)).otherwise(lit(0)) +
            when(col("is_disabling") == True, lit(1)).otherwise(lit(0))
        ).cast(DoubleType()).alias("seriousness_score"),
    )
