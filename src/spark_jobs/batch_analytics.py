"""
Smart Healthcare Big Data Pipeline - PySpark Structured Streaming & NEWS2 Alert Job
Processes patient vitals streams in real-time, calculates clinical NEWS2 scores,
triggers critical deterioration alerts, and computes windowed streaming analytics.
"""

import os
import sys
import json
from datetime import datetime
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, when, lit, current_timestamp,
    window, avg, max as spark_max, min as spark_min, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "patient-vitals")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "patient-alerts")
DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "data/raw")
DATA_PROCESSED_DIR = os.getenv("DATA_PROCESSED_DIR", "data/processed")
DATA_ALERTS_DIR = os.getenv("DATA_ALERTS_DIR", "data/alerts")
RAW_BUFFER_FILE = os.path.join(DATA_RAW_DIR, "streaming_vitals_buffer.jsonl")


def build_spark_session():
    """Initializes and returns a high-performance PySpark Session."""
    return SparkSession.builder \
        .appName("SmartHealthcare-NEWS2-Analytics") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoints/vitals_stream") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()


def get_vitals_schema():
    """Returns the schema for incoming patient vitals messages."""
    return StructType([
        StructField("patient_id", StringType(), False),
        StructField("patient_name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("ward", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("respiration_rate", IntegerType(), True),
        StructField("spo2", DoubleType(), True),
        StructField("supplemental_oxygen", BooleanType(), True),
        StructField("systolic_bp", IntegerType(), True),
        StructField("diastolic_bp", IntegerType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("consciousness", StringType(), True),
        StructField("is_synthetic_anomaly", BooleanType(), True)
    ])


def calculate_news2_score(df):
    """
    Applies the official Royal College of Physicians National Early Warning Score 2 (NEWS2) rules.
    Calculates sub-scores for each parameter and totals them.
    """
    # 1. Respiration Rate Score (breaths per minute)
    # <=8: 3, 9-11: 1, 12-20: 0, 21-24: 2, >=25: 3
    resp_score = when(col("respiration_rate") <= 8, 3) \
        .when((col("respiration_rate") >= 9) & (col("respiration_rate") <= 11), 1) \
        .when((col("respiration_rate") >= 12) & (col("respiration_rate") <= 20), 0) \
        .when((col("respiration_rate") >= 21) & (col("respiration_rate") <= 24), 2) \
        .otherwise(3)

    # 2. Oxygen Saturation (SpO2) Score (Scale 1)
    # <=91: 3, 92-93: 2, 94-95: 1, >=96: 0
    spo2_score = when(col("spo2") <= 91.0, 3) \
        .when((col("spo2") >= 92.0) & (col("spo2") <= 93.0), 2) \
        .when((col("spo2") >= 94.0) & (col("spo2") <= 95.0), 1) \
        .otherwise(0)

    # 3. Supplemental Oxygen Score
    # True: 2, False: 0
    o2_score = when(col("supplemental_oxygen") == True, 2).otherwise(0)

    # 4. Systolic Blood Pressure (mmHg)
    # <=90: 3, 91-100: 2, 101-110: 1, 111-219: 0, >=220: 3
    sbp_score = when(col("systolic_bp") <= 90, 3) \
        .when((col("systolic_bp") >= 91) & (col("systolic_bp") <= 100), 2) \
        .when((col("systolic_bp") >= 101) & (col("systolic_bp") <= 110), 1) \
        .when((col("systolic_bp") >= 111) & (col("systolic_bp") <= 219), 0) \
        .otherwise(3)

    # 5. Heart Rate (beats per minute)
    # <=40: 3, 41-50: 1, 51-90: 0, 91-110: 1, 111-130: 2, >=131: 3
    hr_score = when(col("heart_rate") <= 40, 3) \
        .when((col("heart_rate") >= 41) & (col("heart_rate") <= 50), 1) \
        .when((col("heart_rate") >= 51) & (col("heart_rate") <= 90), 0) \
        .when((col("heart_rate") >= 91) & (col("heart_rate") <= 110), 1) \
        .when((col("heart_rate") >= 111) & (col("heart_rate") <= 130), 2) \
        .otherwise(3)

    # 6. Temperature (Celsius)
    # <=35.0: 3, 35.1-36.0: 1, 36.1-38.0: 0, 38.1-39.0: 1, >=39.1: 2
    temp_score = when(col("temperature") <= 35.0, 3) \
        .when((col("temperature") >= 35.1) & (col("temperature") <= 36.0), 1) \
        .when((col("temperature") >= 36.1) & (col("temperature") <= 38.0), 0) \
        .when((col("temperature") >= 38.1) & (col("temperature") <= 39.0), 1) \
        .otherwise(2)

    # 7. Consciousness (AVPU scale)
    # Alert: 0, CVPU: 3
    avpu_score = when(col("consciousness") == "Alert", 0).otherwise(3)

    # Assemble enriched DataFrame with sub-scores and aggregate NEWS2 total
    df_scored = df.withColumn("score_resp", resp_score) \
        .withColumn("score_spo2", spo2_score) \
        .withColumn("score_o2", o2_score) \
        .withColumn("score_sbp", sbp_score) \
        .withColumn("score_hr", hr_score) \
        .withColumn("score_temp", temp_score) \
        .withColumn("score_avpu", avpu_score)

    total_news2 = (
        col("score_resp") + col("score_spo2") + col("score_o2") +
        col("score_sbp") + col("score_hr") + col("score_temp") + col("score_avpu")
    )
    df_scored = df_scored.withColumn("news2_score", total_news2)

    # Clinical Risk Category & Action Plan Assignment
    # 0-4: Low Risk, 5-6: Medium Risk, >=7: High Risk / Emergency
    risk_level = when(col("news2_score") >= 7, lit("HIGH_EMERGENCY")) \
        .when(col("news2_score") >= 5, lit("MEDIUM_URGENT")) \
        .when((col("score_resp") == 3) | (col("score_spo2") == 3) | (col("score_sbp") == 3) | (col("score_hr") == 3), lit("LOW_MEDIUM_SINGLE_EXTREME")) \
        .otherwise(lit("LOW_NORMAL"))

    clinical_action = when(col("news2_score") >= 7, lit("Emergency ICU / Critical Care Team Deployment")) \
        .when(col("news2_score") >= 5, lit("Urgent Ward Medical Doctor Assessment Required")) \
        .when(col("news2_score") >= 1, lit("Increase Monitoring Frequency to Every 4 Hours")) \
        .otherwise(lit("Routine Observations (12-Hourly)"))

    is_alert = (col("news2_score") >= 5) | (col("score_resp") == 3) | (col("score_spo2") == 3) | (col("score_sbp") == 3) | (col("score_hr") == 3)

    df_scored = df_scored \
        .withColumn("risk_level", risk_level) \
        .withColumn("clinical_action", clinical_action) \
        .withColumn("is_alert", is_alert) \
        .withColumn("processed_time", current_timestamp())

    return df_scored


def process_batch(batch_df, batch_id):
    """Callback for each streaming micro-batch."""
    if batch_df.count() == 0:
        return

    scored_df = calculate_news2_score(batch_df)
    scored_df.persist()

    print(f"\n=======================================================")
    print(f"[SPARK STREAM] Processing Micro-Batch #{batch_id} ({scored_df.count()} events)")
    print(f"=======================================================")

    # Display high-risk patients
    alerts_df = scored_df.filter(col("is_alert") == True)
    alert_count = alerts_df.count()

    print(f"[METRICS] Total Events: {scored_df.count()} | Active Clinical Alerts: {alert_count}")

    if alert_count > 0:
        print("\n>>> ACTIVE CRITICAL CLINICAL ALERTS <<<")
        alerts_df.select(
            "patient_id", "patient_name", "ward", "news2_score",
            "risk_level", "heart_rate", "spo2", "systolic_bp", "clinical_action"
        ).show(truncate=False)

        # Write alerts to alerts directory for Streamlit real-time monitoring
        os.makedirs(DATA_ALERTS_DIR, exist_ok=True)
        alerts_pd = alerts_df.toPandas()
        alerts_file = os.path.join(DATA_ALERTS_DIR, "live_alerts.json")
        alerts_pd.to_json(alerts_file, orient="records", date_format="iso", indent=2)

    # Persist processed batch to processed data directory for historical view & dashboard
    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    processed_pd = scored_df.toPandas()
    recent_file = os.path.join(DATA_PROCESSED_DIR, "recent_vitals_scored.json")
    processed_pd.to_json(recent_file, orient="records", date_format="iso", indent=2)

    scored_df.unpersist()


def run_pipeline():
    """Starts the PySpark Structured Streaming or Batch processing pipeline."""
    print("=================================================================")
    print("   Starting PySpark Clinical Analytics & NEWS2 Alert Pipeline    ")
    print("=================================================================")

    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    os.makedirs(DATA_ALERTS_DIR, exist_ok=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    schema = get_vitals_schema()

    # Determine input source: Kafka or local file buffer
    use_kafka = False
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(','),
            consumer_timeout_ms=3000
        )
        consumer.close()
        use_kafka = True
        print(f"[OK] Successfully verified Kafka connection at {KAFKA_BOOTSTRAP}")
    except Exception:
        print(f"[INFO] Kafka broker not reachable directly. Using local streaming file fallback: {RAW_BUFFER_FILE}")

    if use_kafka:
        print(f"[SOURCE] Subscribing to Kafka topic '{KAFKA_TOPIC}'...")
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        query = parsed_stream.writeStream \
            .foreachBatch(process_batch) \
            .start()

        query.awaitTermination()
    else:
        # Fallback Batch/Streaming from Local Buffer
        if not os.path.exists(RAW_BUFFER_FILE):
            print(f"[INFO] Buffer file {RAW_BUFFER_FILE} not found yet. Generating initial seed vitals...")
            from src.generator.stream_generator import PATIENT_POOL, generate_patient_vitals
            os.makedirs(DATA_RAW_DIR, exist_ok=True)
            with open(RAW_BUFFER_FILE, "w", encoding="utf-8") as f:
                for patient in PATIENT_POOL:
                    for _ in range(5):
                        vitals = generate_patient_vitals(patient, anomaly_prob=0.25)
                        f.write(json.dumps(vitals) + "\n")

        print(f"[SOURCE] Reading patient vitals dataset from {RAW_BUFFER_FILE}...")
        raw_df = spark.read.schema(schema).json(RAW_BUFFER_FILE)
        print(f"[DATA] Loaded {raw_df.count()} vital records. Computing NEWS2 scores...")

        scored_df = calculate_news2_score(raw_df)

        print("\n--- Summary: Clinical Risk Distribution ---")
        scored_df.groupBy("risk_level").count().show()

        print("\n--- Sample Scored Records ---")
        scored_df.select(
            "patient_id", "patient_name", "news2_score", "risk_level",
            "heart_rate", "spo2", "systolic_bp", "clinical_action"
        ).show(10, truncate=False)

        # Save processed results for Streamlit
        scored_pd = scored_df.toPandas()
        recent_file = os.path.join(DATA_PROCESSED_DIR, "recent_vitals_scored.json")
        scored_pd.to_json(recent_file, orient="records", date_format="iso", indent=2)

        alerts_pd = scored_pd[scored_pd["is_alert"] == True]
        alerts_file = os.path.join(DATA_ALERTS_DIR, "live_alerts.json")
        alerts_pd.to_json(alerts_file, orient="records", date_format="iso", indent=2)

        print(f"[SUCCESS] Scored dataset written to: {recent_file}")
        print(f"[SUCCESS] Live alerts written to: {alerts_file}")
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
