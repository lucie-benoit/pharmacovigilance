"""
Pharmacovigilance - BATCH pipeline
====================================================
Runs once on the full FAERS dataset, computes all core indicators required
by Part 1, and stores each result in MongoDB (Part 2).

Schema and column derivation logic live in faers_schema.py (shared with
pharma_streaming.py) so they only need to be defined/maintained once.

Run with:
docker compose up spark-batch
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

from faers_schema import raw_schema, map_columns

# ================ SPARK SESSION ================================

# allocate 2GB for driver and executor each, to avoid OOM errors on larger datasets

spark = SparkSession.builder \
    .appName("Pharmacovigilance-Batch") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ================ LOADING DATA ==================================

df_raw = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(raw_schema) \
    .csv("data/full")

df = map_columns(df_raw)

# Cache once: this DataFrame is the base of every analysis below,
# so we avoid re-reading/re-parsing the CSV for each of them.
df.cache()

row_count = df.count()
print(f"[INFO] Loaded {row_count} rows from data/input")

if row_count == 0:
    print("[WARN] No data found in data/input. Exiting.")
    spark.stop()
    raise SystemExit(0)

# ================ MONGO STORAGE  =================

MONGO_URI = "mongodb://mongo:27017" # MongoDB connection string (docker-compose service name)
MONGO_DATABASE = "pharmacovigilance"


def save_to_mongo(spark_df, collection_name):
    """Write a result DataFrame to a MongoDB collection.
    Wrapped in try/except so one failing write doesn't stop the whole batch."""
    try:
        spark_df.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("connection.uri", MONGO_URI) \
            .option("database", MONGO_DATABASE) \
            .option("collection", collection_name) \
            .save()
        print(f"[OK] Saved collection '{collection_name}'")
    except Exception as e:
        print(f"[ERROR] Failed to save '{collection_name}': {e}")


# ================ PART 1 - CORE ANALYSES =========================

# -------- Drug Analysis ------------

# Top reported drugs
top_drugs = df.groupBy("drug_name") \
    .agg(count("report_id").alias("report_count")) \
    .orderBy(col("report_count").desc())
save_to_mongo(top_drugs, "top_drugs")

# Number of reports by drug
number_of_reports = df.groupBy("drug_name") \
    .agg(count("report_id").alias("num_reports"))
save_to_mongo(number_of_reports, "number_of_reports")

# Number of severe reports by drug
severe_reports = df.filter(col("severity") == "Severe") \
    .groupBy("drug_name") \
    .agg(count("report_id").alias("num_severe_reports"))
save_to_mongo(severe_reports, "severe_reports")

# -------- Adverse Event Analysis ------

# Most frequent adverse events
adverse_events = df.groupBy("adverse_event") \
    .agg(count("adverse_event").alias("event_count")) \
    .orderBy(col("event_count").desc())
save_to_mongo(adverse_events, "adverse_events")

# Most severe adverse events
severe_adverse_events = df.filter(col("severity") == "Severe") \
    .groupBy("adverse_event") \
    .agg(count("report_id").alias("num_severe_events")) \
    .orderBy(col("num_severe_events").desc())
save_to_mongo(severe_adverse_events, "severe_adverse_events")

# -------- Temporal Analysis ------------

# Monthly evolution of reports
monthly_reports = df.withColumn("report_month", col("report_date").substr(1, 7)) \
    .groupBy("report_month") \
    .agg(count("report_id").alias("num_reports")) \
    .orderBy(col("report_month").asc())
save_to_mongo(monthly_reports, "monthly_reports")

# Monthly evolution of severe cases
severe_monthly_reports = df.withColumn("report_month", col("report_date").substr(1, 7)) \
    .filter(col("severity") == "Severe") \
    .groupBy("report_month") \
    .agg(count("report_id").alias("num_severe_reports")) \
    .orderBy(col("report_month").asc())
save_to_mongo(severe_monthly_reports, "severe_monthly_reports")

# -------- Risk Score Computation -----

custom_risk_score = df.groupBy("drug_name") \
    .agg(count("report_id").alias("num_reports"),
         avg("seriousness_score").alias("mean_severity")) \
    .withColumn("risk_score",
                (col("num_reports") * 0.4) + (col("mean_severity") * 0.6)) \
    .orderBy(col("risk_score").desc())
save_to_mongo(custom_risk_score, "risk_score")

# -------- Signal Detection ----------

signal_detection = df.groupBy("drug_name", "adverse_event") \
    .agg(count("report_id").alias("num_reports"),
         avg("seriousness_score").alias("mean_severity")) \
    .withColumn("signal_score",
                (col("num_reports") * 0.3) + (col("mean_severity") * 0.7)) \
    .orderBy(col("signal_score").desc())
save_to_mongo(signal_detection, "signal_detection")

# ========== ADDITIONAL FEATURES ====================================

# -------- Hospitalization Rate by Drug ---------
hospitalization_by_drug = df.groupBy("drug_name") \
    .agg(
        count("report_id").alias("num_reports"),
        avg(col("is_hospitalized").cast("int")).alias("hospitalization_rate")
    ) \
    .filter(col("num_reports") > 10) \
    .orderBy(col("hospitalization_rate").desc())
save_to_mongo(hospitalization_by_drug, "hospitalization_by_drug")

# -------- Fatality Rate by Drug ---------

fatality_by_drug = df.groupBy("drug_name") \
    .agg(
        count("report_id").alias("num_reports"),
        avg(col("is_fatal").cast("int")).alias("fatality_rate")
    ) \
    .filter(col("num_reports") > 10) \
    .orderBy(col("fatality_rate").desc())
save_to_mongo(fatality_by_drug, "fatality_by_drug")

# ============================================================

df.unpersist()
print("[INFO] Batch pipeline finished successfully.")
spark.stop()
