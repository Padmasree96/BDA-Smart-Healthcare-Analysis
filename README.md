# 🏥 Smart Healthcare Big Data Streaming & Predictive Analytics Pipeline

An enterprise-grade Big Data processing and Machine Learning pipeline for real-time patient vital sign monitoring, clinical deterioration detection (**NEWS2 - National Early Warning Score 2**), and 30-day hospital readmission risk prediction using **Apache Kafka**, **Apache Spark (PySpark Structured Streaming & MLlib)**, and **Streamlit**.

---

## 🌟 Key Features

1. **Real-Time Patient Vitals Stream Generator (`src/generator/stream_generator.py`)**:
   - Simulates streaming physiological parameters (Heart Rate, Systolic/Diastolic BP, SpO2, Respiration Rate, Temperature, Consciousness/AVPU).
   - Randomly injects clinical emergencies (Sepsis, Hypoxia, Hypertensive Crisis) to test alerts.
   - Publishes to Apache Kafka topic `patient-vitals` with automatic fallback to local buffer for offline execution.

2. **PySpark Structured Streaming & Clinical Analytics (`src/spark_jobs/batch_analytics.py`)**:
   - Implements official **Royal College of Physicians NEWS2** clinical deterioration logic.
   - Triggers automated alerts for high-risk patients (`NEWS2 >= 5` urgent, `NEWS2 >= 7` ICU emergency).
   - Computes sliding window aggregations and persists processed state to sinks.

3. **Spark MLlib Hospital Readmission Predictor (`src/spark_jobs/ml_risk_prediction.py`)**:
   - Trains a **Random Forest Classifier** pipeline using PySpark MLlib.
   - Evaluates with ROC-AUC, PR-AUC, Accuracy, and F1-Score metrics.
   - Identifies key clinical predictors (NEWS2 score, previous admissions, age, length of stay).

4. **Interactive Clinical Dashboard (`src/dashboard/app.py`)**:
   - Built with Streamlit and Plotly with modern dark-mode medical aesthetics.
   - Live KPI metric cards for Emergency, Urgent, and Stable patient cohorts.
   - Dynamic time-series physiological trajectory charts with danger threshold bands.
   - Interactive 30-day readmission risk calculator with actionable discharge recommendations.

---

## 📁 Project Structure

```
E:\bda\
├── .env.example                  # Environment configuration template
├── .gitignore                    # Git ignore file for Python/Spark/Models
├── docker-compose.yml            # Zookeeper, Kafka broker, and Kafka-UI services
├── Makefile                      # Cross-platform CLI task runner
├── requirements.txt              # Python dependencies
├── run.bat                       # Windows interactive menu script
├── README.md                     # Documentation
├── data/
│   ├── alerts/                   # Triggered clinical emergency alerts
│   ├── models/                   # Serialized MLlib and companion models
│   ├── processed/                # Scored vitals and streaming aggregates
│   └── raw/                      # Raw buffer and historical training datasets
└── src/
    ├── dashboard/
    │   └── app.py                # Streamlit clinical dashboard
    ├── generator/
    │   └── stream_generator.py   # Kafka vitals generator
    └── spark_jobs/
        ├── batch_analytics.py    # PySpark streaming & NEWS2 analytics
        └── ml_risk_prediction.py # Spark MLlib readmission model
```

---

## 🚀 Quick Start Guide

### 1. Environment Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd bda

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Kafka via Docker Compose (Optional)

```bash
docker compose up -d
```
> *Kafka runs on `localhost:9092` and Kafka UI is accessible at `http://localhost:8080`.*

### 3. Run Pipeline via `run.bat` (Windows)

Double click `run.bat` or run:
```cmd
run.bat
```

Or run individual components:

```bash
# 1. Start Vitals Streamer
python src/generator/stream_generator.py

# 2. Run PySpark NEWS2 Streaming Job
python src/spark_jobs/batch_analytics.py

# 3. Train Spark MLlib Readmission Model
python src/spark_jobs/ml_risk_prediction.py

# 4. Launch Clinical Dashboard
streamlit run src/dashboard/app.py
```

---

## 🩺 Clinical NEWS2 Thresholds Reference

| NEWS2 Score | Clinical Risk | Monitoring Frequency | Required Action |
|-------------|---------------|----------------------|-----------------|
| **0 - 4**   | Low Risk      | 12-hourly            | Routine ward observations |
| **3 (Single)** | Low-Medium | 4 to 6-hourly        | Urgent review by ward nurse |
| **5 - 6**   | Medium Risk   | Hourly               | Urgent response by medical doctor |
| **7+**      | High Risk     | Continuous           | Emergency ICU / Critical Care Team |

---

## 📜 License
MIT License. Built for Big Data Analytics research & demonstration.
