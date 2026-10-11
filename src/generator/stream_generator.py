import time
import json
import random
from pathlib import Path

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)
STREAM_FILE = DATA_DIR / "live_vitals_stream.jsonl"

patient_ids = [f"P_{1000 + i}" for i in range(25)]

print("Starting Local Vitals Stream Generator (No Docker needed)...")
print(f"Streaming data to: {STREAM_FILE}")

try:
    while True:
        records = []
        for pid in patient_ids:
            # Random critical spikes simulate panrom
            record = {
                "patient_id": pid,
                "timestamp": int(time.time()),
                "heart_rate": random.randint(55, 135),
                "systolic_bp": random.randint(90, 165),
                "diastolic_bp": random.randint(60, 100),
                "spo2": round(random.uniform(88.0, 99.5), 1),
                "temperature_c": round(random.uniform(36.2, 39.4), 1)
            }
            records.append(record)

        # Append records to JSONL stream buffer
        with open(STREAM_FILE, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        print(f"Generated {len(records)} patient vitals events...")
        time.sleep(2)
except KeyboardInterrupt:
    print("\nStream stopped.")
