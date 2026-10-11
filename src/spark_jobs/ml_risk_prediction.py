"""
Smart Healthcare Big Data Pipeline - Spark MLlib Hospital Readmission Prediction
Builds, trains, evaluates, and saves a Machine Learning pipeline using Spark MLlib
to predict 30-day hospital readmission risk based on patient vitals and clinical history.
"""

import os
import sys
import json
import random
import pandas as pd
import numpy as np
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)

load_dotenv()

DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "data/raw")
DATA_MODELS_DIR = os.getenv("DATA_MODELS_DIR", "data/models")
HISTORICAL_CSV = os.path.join(DATA_RAW_DIR, "patient_historical_encounters.csv")
MODEL_OUTPUT_PATH = os.path.join(DATA_MODELS_DIR, "readmission_rf_model")
METRICS_OUTPUT_PATH = os.path.join(DATA_MODELS_DIR, "model_metrics.json")


def build_spark_session():
    """Initializes and returns a PySpark Session for MLlib."""
    return SparkSession.builder \
        .appName("SmartHealthcare-Readmission-Prediction-ML") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def generate_synthetic_historical_data(file_path, num_records=2500):
    """
    Generates a realistic clinical cohort dataset of past hospital stays
    with demographic, clinical, vital sign, and readmission outcome features.
    """
    print(f"[DATA GEN] Generating {num_records} synthetic patient encounters at {file_path}...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    np.random.seed(42)
    random.seed(42)

    genders = ["Male", "Female"]
    admission_types = ["Emergency", "Urgent", "Elective"]

    records = []
    for i in range(1, num_records + 1):
        patient_id = f"PAT-HIST-{1000 + i}"
        age = int(np.random.normal(62, 16))
        age = max(18, min(95, age))
        gender = random.choice(genders)
        admission_type = random.choice(admission_types)
        time_in_hospital = max(1, int(np.random.exponential(4.5) + 1))
        num_lab_procedures = int(np.random.normal(45, 18))
        num_lab_procedures = max(5, min(120, num_lab_procedures))
        num_medications = int(np.random.normal(16, 8))
        num_medications = max(1, min(65, num_medications))
        num_diagnoses = random.randint(1, 9)
        previous_admissions = int(np.random.poisson(1.2))

        # Physiological summary vitals
        avg_heart_rate = round(float(np.random.normal(78, 14)), 1)
        avg_systolic_bp = round(float(np.random.normal(128, 18)), 1)
        avg_spo2 = round(float(np.random.normal(96.5, 2.5)), 1)
        avg_spo2 = min(100.0, max(82.0, avg_spo2))
        max_news2_score = int(np.random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                               p=[0.20, 0.20, 0.18, 0.14, 0.10, 0.07, 0.05, 0.03, 0.015, 0.01, 0.005]))

        # Comorbidities
        has_diabetes = 1 if (age > 45 and random.random() < 0.42) else 0
        has_hypertension = 1 if (age > 40 and random.random() < 0.55) else 0

        # Readmission probability based on clinical risk factors
        risk_score = (
            (age / 100.0) * 0.25 +
            (time_in_hospital / 15.0) * 0.20 +
            (previous_admissions * 0.15) +
            (max_news2_score / 10.0) * 0.25 +
            (has_diabetes * 0.10) +
            (has_hypertension * 0.05) +
            (1 if admission_type == "Emergency" else 0) * 0.10
        )
        readmission_prob = 1.0 / (1.0 + np.exp(-3.5 * (risk_score - 0.45)))
        readmitted = 1 if random.random() < readmission_prob else 0

        records.append({
            "patient_id": patient_id,
            "age": age,
            "gender": gender,
            "admission_type": admission_type,
            "time_in_hospital": time_in_hospital,
            "num_lab_procedures": num_lab_procedures,
            "num_medications": num_medications,
            "num_diagnoses": num_diagnoses,
            "previous_admissions": previous_admissions,
            "avg_heart_rate": avg_heart_rate,
            "avg_systolic_bp": avg_systolic_bp,
            "avg_spo2": avg_spo2,
            "max_news2_score": max_news2_score,
            "has_diabetes": has_diabetes,
            "has_hypertension": has_hypertension,
            "readmitted": readmitted
        })

    df_hist = pd.DataFrame(records)
    df_hist.to_csv(file_path, index=False)
    print(f"[DATA GEN] Successfully created {len(df_hist)} records. Readmission Rate: {df_hist['readmitted'].mean():.2%}")
    return df_hist


def train_readmission_model():
    """Builds and trains the Spark MLlib classification pipeline."""
    print("=================================================================")
    print("   Starting Spark MLlib Hospital Readmission Prediction Training  ")
    print("=================================================================")

    os.makedirs(DATA_RAW_DIR, exist_ok=True)
    os.makedirs(DATA_MODELS_DIR, exist_ok=True)

    if not os.path.exists(HISTORICAL_CSV):
        generate_synthetic_historical_data(HISTORICAL_CSV, num_records=3000)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SPARK] Loading training data from: {HISTORICAL_CSV}")
    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(HISTORICAL_CSV)
    raw_df.printSchema()

    # Feature definitions
    categorical_cols = ["gender", "admission_type"]
    numerical_cols = [
        "age", "time_in_hospital", "num_lab_procedures",
        "num_medications", "num_diagnoses", "previous_admissions",
        "avg_heart_rate", "avg_systolic_bp", "avg_spo2",
        "max_news2_score", "has_diabetes", "has_hypertension"
    ]
    target_col = "readmitted"

    print("\n--- Cohort Summary Statistics ---")
    raw_df.groupBy(target_col).count().show()

    # Preprocessing Pipeline Stages
    stages = []

    # 1. StringIndexer & OneHotEncoder for Categorical features
    for cat_col in categorical_cols:
        indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_idx", handleInvalid="keep")
        encoder = OneHotEncoder(inputCols=[f"{cat_col}_idx"], outputCols=[f"{cat_col}_vec"])
        stages += [indexer, encoder]

    # 2. VectorAssembler to combine numerical and categorical features
    feature_inputs = numerical_cols + [f"{cat_col}_vec" for cat_col in categorical_cols]
    assembler = VectorAssembler(inputCols=feature_inputs, outputCol="raw_features", handleInvalid="skip")
    stages.append(assembler)

    # 3. Standard Scaler for feature standardization
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
    stages.append(scaler)

    # 4. Classifier: Random Forest Classifier
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol=target_col,
        numTrees=60,
        maxDepth=7,
        seed=42
    )
    stages.append(rf)

    # Create Spark ML Pipeline
    pipeline = Pipeline(stages=stages)

    # Train / Test Split
    train_df, test_df = raw_df.randomSplit([0.80, 0.20], seed=42)
    print(f"[ML PIPELINE] Training records: {train_df.count()} | Test records: {test_df.count()}")

    print("[ML PIPELINE] Fitting Spark ML Pipeline...")
    model = pipeline.fit(train_df)

    # Predict on Test Data
    predictions = model.transform(test_df)

    # Evaluate Model
    roc_evaluator = BinaryClassificationEvaluator(labelCol=target_col, rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    pr_evaluator = BinaryClassificationEvaluator(labelCol=target_col, rawPredictionCol="rawPrediction", metricName="areaUnderPR")
    acc_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="accuracy")
    f1_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="f1")

    auc_roc = roc_evaluator.evaluate(predictions)
    auc_pr = pr_evaluator.evaluate(predictions)
    accuracy = acc_evaluator.evaluate(predictions)
    f1_score = f1_evaluator.evaluate(predictions)

    print("\n=======================================================")
    print("           MODEL EVALUATION PERFORMANCE               ")
    print("=======================================================")
    print(f" Area Under ROC (AUC-ROC) : {auc_roc:.4f}")
    print(f" Area Under PR (AUC-PR)   : {auc_pr:.4f}")
    print(f" Test Accuracy            : {accuracy:.4f}")
    print(f" F1-Score                 : {f1_score:.4f}")
    print("=======================================================\n")

    # Save metrics to JSON for dashboard consumption
    metrics_payload = {
        "model_type": "Spark MLlib RandomForestClassifier",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "auc_roc": round(auc_roc, 4),
        "auc_pr": round(auc_pr, 4),
        "accuracy": round(accuracy, 4),
        "f1_score": round(f1_score, 4),
        "num_trees": 60,
        "max_depth": 7,
        "total_samples": raw_df.count(),
        "train_samples": train_df.count(),
        "test_samples": test_df.count(),
        "feature_names": numerical_cols + categorical_cols
    }

    with open(METRICS_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics_payload, f, indent=2)
    print(f"[SAVED] Metrics exported to: {METRICS_OUTPUT_PATH}")

    # Save trained Spark model
    try:
        model.write().overwrite().save(MODEL_OUTPUT_PATH)
        print(f"[SAVED] Spark MLlib Model saved to: {MODEL_OUTPUT_PATH}")
    except Exception as me:
        print(f"[WARN] Spark model native save: {me}")

    # Also build a lightweight scikit-learn model for super-fast interactive inference in Streamlit
    train_lightweight_sklearn_model(HISTORICAL_CSV)

    spark.stop()
    print("[SUCCESS] Spark MLlib Readmission Prediction Pipeline Completed!")


def train_lightweight_sklearn_model(csv_path):
    """Trains a companion scikit-learn model for millisecond UI inference in Streamlit."""
    try:
        from sklearn.ensemble import RandomForestClassifier as SklearnRF
        import joblib

        df = pd.read_csv(csv_path)
        feature_cols = [
            "age", "time_in_hospital", "num_lab_procedures",
            "num_medications", "num_diagnoses", "previous_admissions",
            "avg_heart_rate", "avg_systolic_bp", "avg_spo2",
            "max_news2_score", "has_diabetes", "has_hypertension"
        ]
        X = df[feature_cols]
        y = df["readmitted"]

        clf = SklearnRF(n_estimators=100, max_depth=8, random_state=42)
        clf.fit(X, y)

        sk_model_path = os.path.join(DATA_MODELS_DIR, "sklearn_readmission.joblib")
        joblib.dump(clf, sk_model_path)

        # Feature importances
        importances = dict(zip(feature_cols, [round(float(x), 4) for x in clf.feature_importances_]))
        feat_path = os.path.join(DATA_MODELS_DIR, "feature_importance.json")
        with open(feat_path, "w", encoding="utf-8") as f:
            json.dump(importances, f, indent=2)

        print(f"[SAVED] Companion inference model saved to: {sk_model_path}")
    except Exception as e:
        print(f"[INFO] Companion model notice: {e}")


if __name__ == "__main__":
    train_readmission_model()
