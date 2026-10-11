"""
Smart Healthcare Big Data Pipeline - Patient Vitals Stream Generator
Simulates real-time clinical vital sign streams and produces messages to Apache Kafka.
Includes graceful local file fallback if Kafka broker is unavailable.
"""

import os
import sys
import json
import time
import random
import argparse
from datetime import datetime, timezone
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "patient-vitals")
RAW_DATA_DIR = os.getenv("DATA_RAW_DIR", "data/raw")
FALLBACK_FILE = os.path.join(RAW_DATA_DIR, "streaming_vitals_buffer.jsonl")

# Patient Cohort Pool
PATIENT_POOL = [
    {"patient_id": "PAT-101", "name": "Ramesh Kumar", "age": 64, "gender": "Male", "ward": "ICU-Bed-01"},
    {"patient_id": "PAT-102", "name": "Priya Sharma", "age": 42, "gender": "Female", "ward": "ICU-Bed-02"},
    {"patient_id": "PAT-103", "name": "Anand Natarajan", "age": 71, "gender": "Male", "ward": "Cardiac-Care-03"},
    {"patient_id": "PAT-104", "name": "Lakshmi Sundaram", "age": 58, "gender": "Female", "ward": "Cardiac-Care-04"},
    {"patient_id": "PAT-105", "name": "David Wilson", "age": 35, "gender": "Male", "ward": "Step-Down-05"},
    {"patient_id": "PAT-106", "name": "Deepa Venkatesh", "age": 69, "gender": "Female", "ward": "Step-Down-06"},
    {"patient_id": "PAT-107", "name": "Karthik Subramanian", "age": 53, "gender": "Male", "ward": "General-Ward-07"},
    {"patient_id": "PAT-108", "name": "Meena Balaji", "age": 77, "gender": "Female", "ward": "General-Ward-08"},
    {"patient_id": "PAT-109", "name": "Suresh Reddy", "age": 49, "gender": "Male", "ward": "General-Ward-09"},
    {"patient_id": "PAT-110", "name": "Kavitha Rajan", "age": 61, "gender": "Female", "ward": "Emergency-Bay-10"}
]


def init_kafka_producer(bootstrap_servers):
    """Attempt to initialize a Kafka producer. Returns None if connection fails."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            request_timeout_ms=5000,
            retries=3
        )
        print(f"[OK] Connected to Kafka Broker at {bootstrap_servers}")
        return producer
    except Exception as e:
        print(f"[WARN] Kafka connection failed ({e}). Running in LOCAL STREAM BUFFER mode.")
        print(f"[INFO] Streaming vitals will be saved to: {FALLBACK_FILE}")
        return None


def generate_patient_vitals(patient, anomaly_prob=0.15):
    """
    Generates physiological vital signs according to standard clinical ranges
    and randomly injects deterioration anomalies to test alert systems.
    """
    is_anomaly = random.random() < anomaly_prob

    if is_anomaly:
        # Clinical Deterioration Event (Severe Sepsis, Hypoxia, or Shock)
        deterioration_type = random.choice(["respiratory_distress", "septic_shock", "hypertensive_crisis"])
        if deterioration_type == "respiratory_distress":
            respiration_rate = random.randint(26, 36)
            spo2 = round(random.uniform(84.0, 91.0), 1)
            supplemental_oxygen = True
            systolic_bp = random.randint(100, 130)
            diastolic_bp = random.randint(65, 85)
            heart_rate = random.randint(115, 145)
            temperature = round(random.uniform(37.5, 38.8), 1)
            consciousness = random.choice(["Alert", "Voice", "Pain"])
        elif deterioration_type == "septic_shock":
            respiration_rate = random.randint(24, 32)
            spo2 = round(random.uniform(88.0, 93.0), 1)
            supplemental_oxygen = True
            systolic_bp = random.randint(70, 88)  # severe hypotension
            diastolic_bp = random.randint(40, 55)
            heart_rate = random.randint(125, 160) # tachycardia
            temperature = round(random.choice([random.uniform(34.5, 35.5), random.uniform(39.2, 40.5)]), 1)
            consciousness = random.choice(["Voice", "Pain", "Unresponsive"])
        else: # hypertensive crisis
            respiration_rate = random.randint(21, 26)
            spo2 = round(random.uniform(93.0, 96.0), 1)
            supplemental_oxygen = False
            systolic_bp = random.randint(220, 245)
            diastolic_bp = random.randint(120, 135)
            heart_rate = random.randint(105, 130)
            temperature = round(random.uniform(36.8, 37.8), 1)
            consciousness = "Alert"
    else:
        # Standard Stable / Mild Vitals
        respiration_rate = random.randint(12, 20)
        spo2 = round(random.uniform(95.0, 99.5), 1)
        supplemental_oxygen = random.choice([False, False, False, True])
        systolic_bp = random.randint(110, 138)
        diastolic_bp = random.randint(70, 88)
        heart_rate = random.randint(62, 95)
        temperature = round(random.uniform(36.4, 37.4), 1)
        consciousness = "Alert"

    vitals_payload = {
        "patient_id": patient["patient_id"],
        "patient_name": patient["name"],
        "age": patient["age"],
        "gender": patient["gender"],
        "ward": patient["ward"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "respiration_rate": respiration_rate,
        "spo2": spo2,
        "supplemental_oxygen": supplemental_oxygen,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "heart_rate": heart_rate,
        "temperature": temperature,
        "consciousness": consciousness,
        "is_synthetic_anomaly": is_anomaly
    }
    return vitals_payload


def stream_vitals(frequency=2.0, anomaly_rate=0.20, max_messages=None):
    """Main loop streaming continuous patient vitals."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    producer = init_kafka_producer(KAFKA_BOOTSTRAP)

    print(f"\n[STREAMING] Starting Patient Vitals Streamer...")
    print(f"[STREAMING] Kafka Topic: {KAFKA_TOPIC} | Interval: {frequency}s | Anomaly Rate: {int(anomaly_rate*100)}%")
    print(f"[STREAMING] Monitoring {len(PATIENT_POOL)} simulated patients. Press Ctrl+C to stop.\n")

    message_count = 0

    try:
        while True:
            for patient in PATIENT_POOL:
                vitals = generate_patient_vitals(patient, anomaly_prob=anomaly_rate)
                payload_str = json.dumps(vitals)

                # 1. Send to Kafka if producer is active
                if producer:
                    try:
                        producer.send(
                            topic=KAFKA_TOPIC,
                            key=vitals["patient_id"],
                            value=vitals
                        )
                    except Exception as kerr:
                        print(f"[KAFKA ERR] {kerr}")

                # 2. Always persist to streaming buffer for local processing & Streamlit
                with open(FALLBACK_FILE, "a", encoding="utf-8") as f:
                    f.write(payload_str + "\n")

                message_count += 1
                status_icon = "CRITICAL" if vitals["is_synthetic_anomaly"] else "NORMAL"
                print(
                    f"[{vitals['timestamp'][:19]}] [{status_icon:8s}] {vitals['patient_id']} ({vitals['patient_name']}): "
                    f"HR={vitals['heart_rate']} bpm, BP={vitals['systolic_bp']}/{vitals['diastolic_bp']}, "
                    f"SpO2={vitals['spo2']}%, RR={vitals['respiration_rate']}, Temp={vitals['temperature']}C"
                )

                if max_messages and message_count >= max_messages:
                    print(f"\n[DONE] Reached target limit of {max_messages} messages.")
                    return

            if producer:
                producer.flush()

            time.sleep(frequency)

    except KeyboardInterrupt:
        print("\n[STOP] Stream generator interrupted by user. Gracefully exiting.")
    finally:
        if producer:
            producer.close()
            print("[INFO] Kafka producer connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Healthcare Patient Vitals Kafka Stream Generator")
    parser.add_argument("--frequency", type=float, default=2.0, help="Seconds between streaming intervals")
    parser.add_argument("--anomaly-rate", type=float, default=0.20, help="Probability of clinical deterioration")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after N messages (optional)")
    args = parser.parse_args()

    stream_vitals(
        frequency=args.frequency,
        anomaly_rate=args.anomaly_rate,
        max_messages=args.max_messages
    )
