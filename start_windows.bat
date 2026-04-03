@echo off
setlocal DisableDelayedExpansion
echo Starting UFDR Analysis Tool Setup...

:: Check for Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python is not installed or not in PATH.
    echo Please install Python 3.9+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)

:: Check Python version
for /f "tokens=2" %%v in ('python --version') do set pyver=%%v
for /f "tokens=1,2 delims=." %%a in ("%pyver%") do (
    set major=%%a
    set minor=%%b
)

if %major% LSS 3 (
    echo Error: Python 3.9+ is required. Found: %pyver%
    pause
    exit /b 1
)
if %major% EQU 3 (
    if %minor% LSS 9 (
        echo Error: Python 3.9+ is required. Found: %pyver%
        pause
        exit /b 1
    )
)

:: Create virtual environment if it doesn't exist
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
    if %errorlevel% neq 0 (
        echo Failed to create virtual environment.
        pause
        exit /b 1
    )
)

:: Activate virtual environment
call venv\Scripts\activate.bat

:: Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

:: Install requirements
echo Installing dependencies... (this may take a few minutes)
pip install -r requirements.txt

:: Setup .env
if not exist ".env" (
    if exist ".env.example" (
        echo Creating default .env file...
        copy .env.example .env >nul
    )
)

:: Run the app
echo Starting application...
streamlit run frontend/app.py
