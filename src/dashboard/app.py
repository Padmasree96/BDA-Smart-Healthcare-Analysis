import streamlit as st
import pandas as pd
import numpy as np
import json
import time
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1. Page Configuration
st.set_page_config(
    page_title="Smart Healthcare | ICU Command Center",
    page_icon="🏥",
    layout="wide"
)

# 2. Modern Clinical Dark Theme Styling
st.markdown("""
<style>
    /* Dark clinical theme */
    .stApp {
        background-color: #0b0f19;
        color: #e2e8f0;
    }
    .main-header {
        background: linear-gradient(90deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 16px 24px;
        margin-bottom: 20px;
    }
    .patient-card {
        border-radius: 10px;
        padding: 14px;
        margin-bottom: 12px;
        border-left: 6px solid #64748b;
        background-color: #1e293b;
    }
    .badge-critical {
        background-color: #ef4444;
        color: white;
        padding: 3px 10px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 11px;
    }
    .badge-warning {
        background-color: #f59e0b;
        color: black;
        padding: 3px 10px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 11px;
    }
    .badge-stable {
        background-color: #10b981;
        color: white;
        padding: 3px 10px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 11px;
    }
</style>
""", unsafe_allow_html=True)

# 3. Data Loader
stream_file = Path("data/raw/live_vitals_stream.jsonl")

if not stream_file.exists():
    st.error("❌ Live Vitals Stream kanavillai. Terminal-la run pannunga: `python src/generator/stream_generator.py`")
    st.stop()

records = []
try:
    with open(stream_file, "r", encoding="utf-8") as f:
        # Read last 600 telemetry points
        for line in f.readlines()[-600:]:
            line_str = line.strip()
            if line_str:
                records.append(json.loads(line_str))
except Exception as e:
    st.error(f"Error reading stream: {e}")
    st.stop()

if not records:
    st.warning("⚠️ Data streaming aagurathukaga wait panrom... Generator run aaguthanu check pannunga.")
    st.stop()

df = pd.DataFrame(records)
df['time'] = pd.to_datetime(df['timestamp'], unit='s').dt.strftime('%H:%M:%S')

# Clinical Scoring (NEWS2 standard based)
def compute_severity(row):
    hr = row['heart_rate']
    spo2 = row['spo2']
    sys_bp = row['systolic_bp']
    
    if spo2 < 90.0 or hr > 125 or hr < 50 or sys_bp > 160:
        return 'CRITICAL', '#ef4444'
    elif spo2 < 93.0 or hr > 105 or sys_bp > 140:
        return 'WARNING', '#f59e0b'
    return 'STABLE', '#10b981'

severity_results = df.apply(compute_severity, axis=1)
df['severity'] = [res[0] for res in severity_results]
df['severity_color'] = [res[1] for res in severity_results]

# Latest status per patient bed
latest = df.groupby('patient_id').last().reset_index()

# 4. Header Bar
st.markdown("""
<div class="main-header">
    <div style="display:flex; justify-content:space-between; align-items:center;">
        <div>
            <h2 style="margin:0; color:#38bdf8;">🏥 ICU Smart Command Center</h2>
            <p style="margin:4px 0 0 0; color:#94a3b8; font-size:14px;">Real-Time Big Data Telemetry & Clinical Deterioration Detection</p>
        </div>
        <div>
            <span style="background-color:#0284c7; color:white; padding:6px 14px; border-radius:8px; font-weight:600; font-size:13px;">
                ● LIVE STREAMING
            </span>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# 5. Ward-Level Metric Cards
crit_patients = latest[latest['severity'] == 'CRITICAL']
warn_patients = latest[latest['severity'] == 'WARNING']
stable_patients = latest[latest['severity'] == 'STABLE']

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("🛌 Monitored ICU Beds", len(latest))
m2.metric("🚨 Critical Emergencies", len(crit_patients), delta=f"{len(crit_patients)} Active" if len(crit_patients)>0 else "Clear", delta_color="inverse")
m3.metric("⚠️ Moderate Warnings", len(warn_patients))
m4.metric("🫀 Ward Avg Pulse", f"{int(latest['heart_rate'].mean())} bpm")
m5.metric("🫁 Ward Avg SpO₂", f"{latest['spo2'].mean():.1f}%")

st.markdown("<br>", unsafe_allow_html=True)

# 6. Main Dashboard Layout (Split View: Bed Selector + Live Graph & Priority Ward Triage)
col_left, col_right = st.columns([2.2, 1.2])

with col_left:
    st.markdown("### 🫀 Individual Bed Vital Telemetry")
    
    # Patient selector
    selected_patient = st.selectbox(
        "Select ICU Bed / Patient ID to inspect:",
        options=sorted(latest['patient_id'].unique()),
        index=0
    )
    
    p_data = df[df['patient_id'] == selected_patient].tail(30)
    current_p = latest[latest['patient_id'] == selected_patient].iloc[0]
    
    # Current stats mini-strip for selected patient
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Pulse Rate", f"{current_p['heart_rate']} bpm")
    s2.metric("Oxygen (SpO2)", f"{current_p['spo2']}%")
    s3.metric("Blood Pressure", f"{current_p['systolic_bp']}/{current_p['diastolic_bp']} mmHg")
    s4.metric("Temperature", f"{current_p['temperature_c']} °C")
    
    # Multi-vital Plotly Chart (Pulse + SpO2 + Blood Pressure)
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("Heart Rate (bpm) & Blood Oxygen (SpO2 %)", "Systolic Blood Pressure (mmHg)")
    )
    
    # Pulse Trace
    fig.add_trace(
        go.Scatter(x=p_data['time'], y=p_data['heart_rate'], name="Pulse (HR)", line=dict(color="#ef4444", width=3)),
        row=1, col=1
    )
    # SpO2 Trace
    fig.add_trace(
        go.Scatter(x=p_data['time'], y=p_data['spo2'], name="SpO2 (%)", line=dict(color="#38bdf8", width=3, dash='dot')),
        row=1, col=1
    )
    # Systolic BP Trace
    fig.add_trace(
        go.Bar(x=p_data['time'], y=p_data['systolic_bp'], name="Sys BP", marker_color="#f59e0b", opacity=0.7),
        row=2, col=1
    )
    
    fig.update_layout(
        template="plotly_dark",
        height=380,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.markdown("### 🚨 Urgent Action Triage")
    
    if len(crit_patients) == 0 and len(warn_patients) == 0:
        st.success("✅ All patients are currently in STABLE condition.")
    else:
        # Emergency alerts list
        for _, p in crit_patients.iterrows():
            st.markdown(f"""
            <div style="background-color:#450a0a; border: 1px solid #dc2626; border-radius:8px; padding:12px; margin-bottom:10px;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <strong style="color:#fecaca; font-size:15px;">🛏️ {p['patient_id']}</strong>
                    <span class="badge-critical">CRITICAL</span>
                </div>
                <div style="font-size:13px; color:#fca5a5; margin-top:6px;">
                    HR: <b>{p['heart_rate']} bpm</b> | SpO₂: <b>{p['spo2']}%</b> | BP: <b>{p['systolic_bp']}/{p['diastolic_bp']}</b>
                </div>
                <div style="font-size:11px; color:#f87171; margin-top:4px;">
                    ⚠️ Immediate Attending Physician Review Required
                </div>
            </div>
            """, unsafe_allow_html=True)
            
        for _, p in warn_patients.head(3).iterrows():
            st.markdown(f"""
            <div style="background-color:#451a03; border: 1px solid #d97706; border-radius:8px; padding:10px; margin-bottom:10px;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <strong style="color:#fde68a; font-size:14px;">🛏️ {p['patient_id']}</strong>
                    <span class="badge-warning">WARNING</span>
                </div>
                <div style="font-size:12px; color:#fed7aa; margin-top:4px;">
                    HR: <b>{p['heart_rate']} bpm</b> | SpO₂: <b>{p['spo2']}%</b>
                </div>
            </div>
            """, unsafe_allow_html=True)

st.markdown("---")

# 7. Bed Ward Matrix Overview (Visual 25 Beds Grid)
st.markdown("### 🏥 Ward ICU Bed Matrix (Live Patient Status)")

grid_cols = st.columns(5)
for idx, (_, row) in enumerate(latest.iterrows()):
    col = grid_cols[idx % 5]
    badge_class = f"badge-{row['severity'].lower()}"
    with col:
        st.markdown(f"""
        <div style="background-color:#1e293b; border-radius:8px; padding:10px; margin-bottom:8px; border-left:4px solid {row['severity_color']};">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <b style="font-size:13px;">{row['patient_id']}</b>
                <span class="{badge_class}">{row['severity']}</span>
            </div>
            <div style="font-size:11px; color:#94a3b8; margin-top:4px;">
                HR: {row['heart_rate']} | SpO₂: {row['spo2']}%
            </div>
        </div>
        """, unsafe_allow_html=True)

# 8. Smooth Live Refresh Loop (Every 2 seconds)
time.sleep(2)
st.rerun()
