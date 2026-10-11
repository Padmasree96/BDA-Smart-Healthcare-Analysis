import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("SmartHealthcare-LocalAnalytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

stream_path = "data/raw"
alert_path = Path("data/alerts")
alert_path.mkdir(parents=True, exist_ok=True)

schema = StructType() \
    .add("patient_id", StringType()) \
    .add("timestamp", LongType()) \
    .add("heart_rate", IntegerType()) \
    .add("systolic_bp", IntegerType()) \
    .add("diastolic_bp", IntegerType()) \
    .add("spo2", DoubleType()) \
    .add("temperature_c", DoubleType())

print("PySpark Local Engine listening on patient stream...")

# Read stream directly from file directory
raw_df = spark.readStream \
    .schema(schema) \
    .json(stream_path)

# Early Warning Critical Indicator
analyzed_df = raw_df.withColumn(
    "alert_level",
    when((col("spo2") < 90.0) | (col("heart_rate") > 125) | (col("systolic_bp") > 155), "CRITICAL")
    .when((col("spo2") < 93.0) | (col("heart_rate") > 105), "WARNING")
    .otherwise("STABLE")
)

# Output critical vitals to console
query = analyzed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
