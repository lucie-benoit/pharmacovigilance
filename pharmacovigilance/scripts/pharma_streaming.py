"""
Pharmacovigilance - STREAMING extension
=========================================================================
Watches data/input for new/arriving CSV files (chunks of the FAERS
dataset, see split_dataset.py) and continuously refreshes demographic
and manufacturer indicators in MongoDB.

Schema and column derivation logic live in faers_schema.py (shared with
pharma_batch.py) so they only need to be defined/maintained once.

These four analyses were chosen for streaming because they are the
lightest (single groupBy, low-cardinality keys), which keeps the
maintained state small and avoids the memory pressure that caused an
earlier crash when all analyses ran as streaming queries at once.

Run with:
docker compose up spark-streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

from faers_schema import raw_schema, map_columns

# ================ SPARK SESSION ================================

spark = SparkSession.builder \
    .appName("Pharmacovigilance-Streaming") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ================ LOADING DATA (STREAMING SOURCE) ================

df_stream_raw = spark.readStream \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("maxFilesPerTrigger", 1) \
    .schema(raw_schema) \
    .csv("data/input")

# we only keep the columns needed
# for the streaming analyses (age_group, manufacturer, seriousness_score).
df_stream = map_columns(df_stream_raw).select(
    "report_id", "age_group", "manufacturer", "seriousness_score"
)

# ================ MONGO STORAGE ============================

MONGO_URI = "mongodb://mongo:27017"
MONGO_DATABASE = "pharmacovigilance"


def write_to_mongo(collection_name):
    """Returns a foreachBatch function that overwrites a Mongo collection
    with the current (cumulative, since outputMode=complete) aggregation
    result for this micro-batch."""
    def _write(batch_df, batch_id):
        try:
            if batch_df.count() > 0:
                batch_df.write \
                    .format("mongodb") \
                    .mode("overwrite") \
                    .option("connection.uri", MONGO_URI) \
                    .option("database", MONGO_DATABASE) \
                    .option("collection", collection_name) \
                    .save()
                print(f"[OK] batch {batch_id} -> '{collection_name}' updated")
        except Exception as e:
            print(f"[ERROR] batch {batch_id} -> '{collection_name}' failed: {e}")
    return _write


# ================ ADDITIONAL FEATURES =================

# -------- Age Group Analysis ---------

# Population at risk by age group
population_at_risk = df_stream.groupBy("age_group") \
    .agg(count("report_id").alias("num_reports"))

query_population_at_risk = population_at_risk.writeStream \
    .foreachBatch(write_to_mongo("population_at_risk")) \
    .outputMode("complete") \
    .trigger(processingTime="30 seconds") \
    .start()

# Mean severity by age group
mean_severity_by_age = df_stream.groupBy("age_group") \
    .agg(avg("seriousness_score").alias("mean_severity"))

query_mean_severity_by_age = mean_severity_by_age.writeStream \
    .foreachBatch(write_to_mongo("mean_severity_by_age")) \
    .outputMode("complete") \
    .trigger(processingTime="30 seconds") \
    .start()

# -------- Manufacturer Analysis ---------

# Number of reports by manufacturer
reports_by_manufacturer = df_stream.groupBy("manufacturer") \
    .agg(count("report_id").alias("num_reports"))

query_reports_by_manufacturer = reports_by_manufacturer.writeStream \
    .foreachBatch(write_to_mongo("reports_by_manufacturer")) \
    .outputMode("complete") \
    .trigger(processingTime="30 seconds") \
    .start()

# Risk score by manufacturer
risk_by_manufacturer = df_stream.groupBy("manufacturer") \
    .agg(count("report_id").alias("num_reports"),
         avg("seriousness_score").alias("mean_severity")) \
    .withColumn("risk_score",
                (col("num_reports") * 0.4) + (col("mean_severity") * 0.6))

query_risk_by_manufacturer = risk_by_manufacturer.writeStream \
    .foreachBatch(write_to_mongo("risk_by_manufacturer")) \
    .outputMode("complete") \
    .trigger(processingTime="30 seconds") \
    .start()

# ================ RUN ============================================

print("[INFO] Streaming pipeline started. Watching data/input for new files...")
spark.streams.awaitAnyTermination()
