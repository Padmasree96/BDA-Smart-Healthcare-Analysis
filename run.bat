@echo off
TITLE Smart Healthcare Big Data Pipeline
COLOR 0A

:MENU
cls
echo =====================================================================
echo          SMART HEALTHCARE BIG DATA PIPELINE CONTROLLER
echo =====================================================================
echo  [1] Start Kafka & Zookeeper (Docker Compose)
echo  [2] Stop Kafka & Zookeeper Containers
echo  [3] Run Patient Vitals Stream Generator (Kafka Producer)
echo  [4] Run PySpark Real-Time Streaming & NEWS2 Alert Job
echo  [5] Run Spark MLlib Readmission Risk Model Training
echo  [6] Launch Streamlit Health Monitoring Dashboard
echo  [7] Run End-to-End Test (Generator + Spark + Dashboard)
echo  [8] Install Requirements
echo  [9] Exit
echo =====================================================================
set /p opt="Select an option [1-9]: "

if "%opt%"=="1" goto START_DOCKER
if "%opt%"=="2" goto STOP_DOCKER
if "%opt%"=="3" goto RUN_GENERATOR
if "%opt%"=="4" goto RUN_SPARK_STREAMING
if "%opt%"=="5" goto RUN_SPARK_ML
if "%opt%"=="6" goto RUN_DASHBOARD
if "%opt%"=="7" goto RUN_ALL
if "%opt%"=="8" goto INSTALL_REQ
if "%opt%"=="9" goto END

echo Invalid selection, please try again.
pause
goto MENU

:START_DOCKER
echo Starting Docker containers (Kafka, Zookeeper, Kafka UI)...
docker compose up -d
echo Kafka is running on localhost:9092. Kafka UI on http://localhost:8080
pause
goto MENU

:STOP_DOCKER
echo Stopping Docker containers...
docker compose down
pause
goto MENU

:RUN_GENERATOR
echo Starting Kafka Patient Vitals Generator...
python src\generator\stream_generator.py
pause
goto MENU

:RUN_SPARK_STREAMING
echo Starting PySpark Streaming & NEWS2 Alert Job...
python src\spark_jobs\batch_analytics.py
pause
goto MENU

:RUN_SPARK_ML
echo Training Spark MLlib Readmission Risk Prediction Model...
python src\spark_jobs\ml_risk_prediction.py
pause
goto MENU

:RUN_DASHBOARD
echo Launching Streamlit Dashboard...
streamlit run src\dashboard\app.py
pause
goto MENU

:RUN_ALL
echo Starting Generator in background...
start cmd /k python src\generator\stream_generator.py
timeout /t 3
echo Starting Spark Streaming in background...
start cmd /k python src\spark_jobs\batch_analytics.py
timeout /t 3
echo Launching Streamlit Dashboard...
start cmd /k streamlit run src\dashboard\app.py
goto MENU

:INSTALL_REQ
echo Installing requirements...
pip install -r requirements.txt
pause
goto MENU

:END
echo Exiting...
exit /b 0
