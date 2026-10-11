import streamlit as st
import pandas as pd
import numpy as np
import json
import time
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Smart Healthcare | ICU Command Center",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (Hospital Dark Theme)
st.markdown("""
<style>
    .metric-card {
        background-color: #1E222D;
        border: 1px solid #2E3440;
        border-radius: 10px;
        padding: 16px;
        text-align: center;
    }
    .metric-value {
        font-size: 28px;
        font-weight: bold;
    }
    .metric-label {
        font-size: 13px;
        color: #8F9CAE;
    }
    .status-badge-crit {
        background-color: #FF4B4B;
        color: white;
        padding: 4px 8px;
        border-radius: 6px;
        font-weight: bold;
        font-size: 12px;
    }
    .status-badge-warn {
        background-color: #FFA500;
        color: black;
        padding: 4px 8px;
        border-radius: 6px;
        font-weight: bold;
        font-size: 12px;
    }
    .status-badge-stable {
        background-color: #00CC96;
        color: white;
        padding: 4px 8px;
        border-radius: 6px;
        font-weight: bold;
        font-size: 12px;
    }
</style>
""", unsafe_allow_html=True)

stream_file = Path("data/raw/live_vitals_stream.jsonl")

# Sidebar Controls
st.sidebar.title("🏥 ICU Ward Controller")
refresh_rate = st.sidebar.slider("Auto-Refresh Rate (seconds)", 1, 5, 2)
st.sidebar.markdown("---")
st.sidebar.subheader("Clinical Alert Thresholds")
st.sidebar.markdown("- **Critical Heart Rate:** > 120 bpm")
st.sidebar.markdown("- **Hypoxia Alert (SpO2):** < 90%")
st.sidebar.markdown("- **Hypertension:** Systolic > 150 mmHg")

# Read buffer data
if not stream_file.exists():
    st.warning("⚠️ Waiting for stream generator... Run: `python src/generator/stream_generator.py`")
    st.stop()

records = []
with open(stream_file, "r", encoding="utf-8") as f:
    for line in f.readlines()[-400:]:
        if line.strip():
            try:
                records.append(json.loads(line))
            except Exception:
                continue

if not records:
    st.info("No records captured yet.")
    st.stop()

df = pd.DataFrame(records)
df['time_str'] = pd.to_datetime(df['timestamp'], unit='s').dt.strftime('%H:%M:%S')

# Clinical Alert Classification
def classify_status(row):
    if row['spo2'] < 90.0 or row['heart_rate'] > 125 or row['systolic_bp'] > 155:
        return 'CRITICAL'
    elif row['spo2'] < 93.0 or row['heart_rate'] > 105:
        return 'WARNING'
    return 'STABLE'

df['status'] = df.apply(classify_status, axis=1)

# Header
st.title("🩺 Smart Healthcare: Real-Time ICU Patient Telemetry")
st.markdown("Automated clinical deterioration alerts and streaming analytics powered by PySpark.")

# Top Metrics Row
latest_records = df.groupby('patient_id').last().reset_index()
total_patients = len(latest_records)
crit_count = len(latest_records[latest_records['status'] == 'CRITICAL'])
warn_count = len(latest_records[latest_records['status'] == 'WARNING'])
avg_spo2 = latest_records['spo2'].mean()
avg_hr = latest_records['heart_rate'].mean()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Monitored Beds", total_patients)
c2.metric("Critical Alerts", crit_count, delta="Action Needed" if crit_count > 0 else "Normal", delta_color="inverse")
c3.metric("Warning Cases", warn_count, delta_color="off")
c4.metric("Avg Ward SpO₂", f"{avg_spo2:.1f}%")
c5.metric("Avg Heart Rate", f"{int(avg_hr)} bpm")

st.markdown("---")

# Main Dashboard: Layout with 2 Columns
col_left, col_right = st.columns([1.8, 1.2])

with col_left:
    st.subheader("📈 Live Patient Vitals Tracker")
    selected_patient = st.selectbox("Select Patient Bed to Inspect:", sorted(df['patient_id'].unique()))
    
    patient_df = df[df['patient_id'] == selected_patient].tail(25)
    
    # Interactive Multi-Axis Chart for HR and SpO2
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=patient_df['time_str'], y=patient_df['heart_rate'],
        mode='lines+markers', name='Heart Rate (bpm)',
        line=dict(color='#FF4B4B', width=3)
    ))
    fig.add_trace(go.Scatter(
        x=patient_df['time_str'], y=patient_df['spo2'],
        mode='lines+markers', name='SpO2 (%)',
        line=dict(color='#00CC96', width=3), yaxis='y2'
    ))
    
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(title='Heart Rate (bpm)'),
        yaxis2=dict(title='SpO2 (%)', overlaying='y', side='right', range=[80, 100]),
        height=340
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("🚨 Priority Action Required (ICU Beds)")
    critical_df = latest_records[latest_records['status'] != 'STABLE'][['patient_id', 'heart_rate', 'spo2', 'systolic_bp', 'status']]
    
    if critical_df.empty:
        st.success("All patient telemetry is within normal clinical thresholds.")
    else:
        st.dataframe(
            critical_df.rename(columns={
                'patient_id': 'Patient ID',
                'heart_rate': 'HR (bpm)',
                'spo2': 'SpO2 (%)',
                'systolic_bp': 'Sys BP',
                'status': 'Severity'
            }),
            use_container_width=True,
            hide_index=True
        )

# Bottom Section: Overall Ward Vital Distribution
st.subheader("📊 Ward-Wide Vital Distribution")
w1, w2 = st.columns(2)

with w1:
    fig_hr = px.histogram(latest_records, x="heart_rate", nbins=15, title="Heart Rate Distribution",
                          color_discrete_sequence=['#FFA15A'], template="plotly_dark")
    fig_hr.update_layout(height=260, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_hr, use_container_width=True)

with w2:
    fig_spo2 = px.histogram(latest_records, x="spo2", nbins=15, title="Blood Oxygen (SpO2) Distribution",
                            color_discrete_sequence=['#636EFA'], template="plotly_dark")
    fig_spo2.update_layout(height=260, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_spo2, use_container_width=True)

# Auto-refresh loop
time.sleep(refresh_rate)
st.rerun()
