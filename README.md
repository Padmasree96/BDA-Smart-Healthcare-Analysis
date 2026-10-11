# 🏥 Smart Healthcare: Big Data Patient Monitoring & Analytics

Lightweight, high-performance Smart Healthcare Analytics Pipeline using **PySpark (Local Mode)** and **Streamlit**. **No Docker required!**

---

## 🚀 Quick Start Guide (Just 2 Steps for Teammates)

### 1. Install Requirements
```bash
pip install -r requirements.txt
```

### 2. Run the System

**Terminal 1:** Start Live Vitals Stream Generator
```bash
python src/generator/stream_generator.py
```

**Terminal 2:** Launch Interactive Dashboard
```bash
streamlit run src/dashboard/app.py
```

---

## ⚡ PySpark Big Data Engine (Viva / Demonstration)

To demonstrate real-time PySpark Structured Streaming with early warning indicators:
```bash
python src/spark_jobs/batch_analytics.py
```

---

## 📁 Repository Structure

```
├── requirements.txt              # Minimal Python dependencies
├── run.bat                       # Windows 1-click launcher
├── README.md                     # Documentation
├── data/
│   ├── raw/                      # Live vitals JSONL stream buffer
│   └── alerts/                   # Triggered alert logs
└── src/
    ├── generator/
    │   └── stream_generator.py   # Continuous vitals generator
    ├── spark_jobs/
    │   └── batch_analytics.py    # PySpark streaming analytics engine
    └── dashboard/
        └── app.py                # Streamlit clinical monitoring UI
```
