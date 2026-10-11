import streamlit as st
import pandas as pd
import json
from pathlib import Path

st.set_page_config(page_title="Smart Healthcare Analytics", layout="wide")
st.title("🏥 Smart Healthcare: Patient Monitoring & Analysis")

stream_file = Path("data/raw/live_vitals_stream.jsonl")

if not stream_file.exists():
    st.warning("Data stream not started yet. Run: python src/generator/stream_generator.py")
    st.stop()

# Read last 150 vitals records
data = []
with open(stream_file, "r", encoding="utf-8") as f:
    for line in f.readlines()[-150:]:
        if line.strip():
            data.append(json.loads(line))

df = pd.DataFrame(data)

if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    critical_count = len(df[(df['spo2'] < 92.0) | (df['heart_rate'] > 120)])
    
    col1.metric("Active Patients", df['patient_id'].nunique())
    col2.metric("Critical Alerts", critical_count)
    col3.metric("Avg Ward SpO2", f"{df['spo2'].mean():.1f}%")
    col4.metric("Avg Heart Rate", f"{int(df['heart_rate'].mean())} bpm")

    st.subheader("Patient Vitals Live Stream")
    st.dataframe(df.tail(10), use_container_width=True)

    st.subheader("Heart Rate & SpO2 Trend")
    st.line_chart(df.set_index("timestamp")[["heart_rate", "spo2"]].tail(30))
