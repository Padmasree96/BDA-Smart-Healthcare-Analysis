.PHONY: up down generator spark-streaming spark-ml dashboard install clean help

help:
	@echo "Available commands:"
	@echo "  make up               - Start Kafka & Zookeeper via Docker Compose"
	@echo "  make down             - Stop Docker containers"
	@echo "  make generator        - Run patient vitals stream generator (Kafka producer)"
	@echo "  make spark-streaming  - Run PySpark streaming & NEWS2 analytics job"
	@echo "  make spark-ml         - Run Spark MLlib readmission prediction model"
	@echo "  make dashboard        - Run Streamlit clinical monitoring dashboard"
	@echo "  make install          - Install Python dependencies"
	@echo "  make clean            - Remove cached data and checkpoints"

install:
	pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

generator:
	python src/generator/stream_generator.py

spark-streaming:
	python src/spark_jobs/batch_analytics.py

spark-ml:
	python src/spark_jobs/ml_risk_prediction.py

dashboard:
	streamlit run src/dashboard/app.py

clean:
	rm -rf checkpoint checkpoints __pycache__ src/*/__pycache__
