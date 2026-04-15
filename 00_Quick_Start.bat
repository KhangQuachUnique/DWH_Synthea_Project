@echo off
REM ============================================================================
REM SYNTHEA ETL - Quick Start Script (Windows)
REM ============================================================================

echo.
echo ============================================================================
echo SYNTHEA ETL - QUICK START
echo ============================================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found. Please install Python 3.8+ first.
    echo Download from: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1] Checking Python version...
python --version

echo.
echo [2] Installing required Python packages...
pip install -r requirements.txt

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
echo [4] Verifying SQL Server connection...
python -c "import pyodbc; print('[SUCCESS] pyodbc is ready')"

echo.
echo ============================================================================
echo [STEP 1] Run SQL Scripts in SSMS
echo ============================================================================
echo Please open these files in SQL Server Management Studio and execute:
echo   1. 01_Schema_Landing.sql    (Create Landing Database)
echo   2. 03_Schema_Staging.sql    (Create Staging Database)
echo.
echo Then come back here and press any key to continue...
pause

echo.
echo ============================================================================
echo [STEP 2] Running ETL Script
echo ============================================================================
echo.
python 02_ETL_Synthea_Extract.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] ETL script failed. Check logs/ directory for details.
    pause
    exit /b 1
)

echo.
echo ============================================================================
echo [SUCCESS] ETL Complete!
echo ============================================================================
echo.
echo Logs saved to: logs/
echo Summary: Check logs/etl_execution_*.csv
echo.
pause
