@echo off
setlocal
REM ============================================================================
REM SYNTHEA ETL - Quick Start Script (Windows)
REM ============================================================================

echo.
echo ============================================================================
echo SYNTHEA ETL - QUICK START
echo ============================================================================
echo.

REM Check if .env file exists (first time setup)
if not exist ".env" (
    echo [WARNING] .env file not found. Copying from .env.example...
    copy .env.example .env >nul 2>&1
    echo [INFO] Please edit .env file with your SQL Server settings before continuing.
    echo [INFO] Press any key to open .env in notepad...
    pause >nul
    notepad .env
)

REM Load environment variables from .env (if exists)
if exist ".env" (
    for /f "tokens=*" %%i in (.env) do set %%i 2>nul
)

REM Check if Python is installed
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found. Please install Python 3.9+ first.
    echo Download from: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Enforce Python >= 3.9 (required by pinned pandas)
python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,9) else 1)" >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python 3.9+ is required.
    python --version
    pause
    exit /b 1
)

echo [1] Checking Python version...
python --version

echo.
echo [2] Installing required Python packages...
python -m pip install -r requirements.txt

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to install packages.
    pause
    exit /b 1
)

echo.
echo [3] Creating required directories...
if not exist "logs" mkdir logs
if not exist "logs\errors" mkdir logs\errors
echo ✓ Created logs/ and logs/errors/ directories

echo.
echo [4] Running pre-flight validation...
python 04_Validation.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Validation failed. Fix the issues above and try again.
    pause
    exit /b 1
)

echo.
echo ============================================================================
echo [STEP 1] Run SQL Scripts in SSMS
echo ============================================================================
echo Please open these files in SQL Server Management Studio and execute:
echo   1. 01_Schema_Landing.sql    (Create Landing tables)
echo   2. 03_Schema_Staging.sql    (Create Staging tables)
echo.
echo Then come back here and press any key to continue...
pause

echo.
echo ============================================================================
echo [STEP 2] Extract: CSV -> Landing
echo ============================================================================
echo.
python 02_ETL_Synthea_Extract.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Extract failed. Check logs/ directory for details.
    pause
    exit /b 1
)

echo.
echo ============================================================================
echo [STEP 3] Transform: Landing -> Staging
echo ============================================================================
echo.
python 05_Transform_Landing_to_Staging.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Transform failed. Check logs/ directory for details.
    pause
    exit /b 1
)

echo.
echo ============================================================================
echo [SUCCESS] ETL Complete!
echo ============================================================================
echo.
echo Logs saved to: logs/
echo Extract summary:   logs/etl_execution_*.csv
echo Transform logs:    logs/transform_*.log
echo.
pause
endlocal
