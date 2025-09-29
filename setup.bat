@echo off
echo ========================================
echo UFDR Analysis Tool - Setup Script
echo ========================================
echo.

echo [1/6] Upgrading pip...
python -m pip install --upgrade pip

echo.
echo [2/6] Installing core dependencies...
python -m pip install streamlit pandas numpy faker python-dotenv pydantic

echo.
echo [3/6] Installing XML and data processing...
python -m pip install lxml xmltodict phonenumbers python-dateutil

echo.
echo [4/6] Installing cloud storage...
python -m pip install boto3 azure-storage-blob cryptography

echo.
echo [5/6] Installing visualization and utilities...
python -m pip install plotly loguru pyyaml click neo4j

echo.
echo [6/6] Installing NLP components (this may take a while)...
python -m pip install sentence-transformers

echo.
echo Optional: Installing FAISS (may require additional setup)
echo To install FAISS, run: pip install faiss-cpu
echo If that fails, you can use: conda install -c pytorch faiss-cpu

echo.
echo ========================================
echo Setup complete!
echo ========================================
echo.
echo Next steps:
echo 1. Run: python run.py setup
echo 2. Run: python run.py samples
echo 3. Run: python run.py
echo.
pause