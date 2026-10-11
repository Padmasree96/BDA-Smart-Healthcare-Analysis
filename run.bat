@echo off
TITLE Smart Healthcare Big Data Pipeline
COLOR 0A

:MENU
cls
echo =====================================================================
echo          SMART HEALTHCARE BIG DATA PIPELINE CONTROLLER
echo =====================================================================
echo  [1] Install Dependencies (pip install -r requirements.txt)
echo  [2] Start Patient Vitals Stream Generator
echo  [3] Run PySpark Local Streaming & Analytics
echo  [4] Launch Streamlit Health Monitoring Dashboard
echo  [5] Run End-to-End Demo (Generator + Dashboard)
echo  [6] Exit
echo =====================================================================
set /p opt="Select an option [1-6]: "

if "%opt%"=="1" goto INSTALL_REQ
if "%opt%"=="2" goto RUN_GENERATOR
if "%opt%"=="3" goto RUN_SPARK
if "%opt%"=="4" goto RUN_DASHBOARD
if "%opt%"=="5" goto RUN_ALL
if "%opt%"=="6" goto END

echo Invalid selection, please try again.
pause
goto MENU

:INSTALL_REQ
echo Installing requirements...
pip install -r requirements.txt
pause
goto MENU

:RUN_GENERATOR
echo Starting Local Patient Vitals Generator...
python src\generator\stream_generator.py
pause
goto MENU

:RUN_SPARK
echo Starting PySpark Local Analytics Job...
python src\spark_jobs\batch_analytics.py
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
echo Launching Streamlit Dashboard...
start cmd /k streamlit run src\dashboard\app.py
goto MENU

:END
echo Exiting...
exit /b 0
