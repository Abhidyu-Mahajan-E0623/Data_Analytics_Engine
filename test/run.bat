@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%" || (echo [ERROR] Failed to enter test directory.& pause & exit /b 1)

set "REQ_FILE=requirements.txt"

:: Find Python
set "PYTHON="
where py >nul 2>&1 && set "PYTHON=py -3"
if not defined PYTHON (
    where python >nul 2>&1 && set "PYTHON=python"
)
if not defined PYTHON (
    echo [ERROR] Python is not installed or not available in PATH.
    pause
    exit /b 1
)

:: Install dependencies
echo [INFO] Installing dependencies...
%PYTHON% -m pip install -r "%REQ_FILE%" -q
if errorlevel 1 (
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
)

:: Run the client
echo.
echo ======================================================
echo         Starting Schema Maker API Client
echo ======================================================
echo.

%PYTHON% client.py

pause
