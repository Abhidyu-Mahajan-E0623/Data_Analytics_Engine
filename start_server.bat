@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%" || (echo [ERROR] Failed to enter project directory.& pause & exit /b 1)

set "VENV_DIR=.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "PIP_EXE=%VENV_DIR%\Scripts\pip.exe"
set "REQ_FILE=requirements.txt"

if not exist "%REQ_FILE%" (
    echo [ERROR] requirements.txt not found in %ROOT_DIR%
    pause
    exit /b 1
)

:: Find Python
set "BASE_PYTHON="
where py >nul 2>&1 && set "BASE_PYTHON=py -3"
if not defined BASE_PYTHON (
    where python >nul 2>&1 && set "BASE_PYTHON=python"
)
if not defined BASE_PYTHON (
    echo [ERROR] Python is not installed or not available in PATH.
    pause
    exit /b 1
)

:: Create venv if missing
if not exist "%PYTHON_EXE%" (
    echo [INFO] Creating virtual environment in %VENV_DIR%...
    %BASE_PYTHON% -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
)

:: Install deps if needed
echo [INFO] Ensuring dependencies are installed...
"%PIP_EXE%" install -r "%REQ_FILE%" -q
if errorlevel 1 (
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
)

:: Start the API server
echo.
echo ======================================================
echo          Starting Schema Maker API Server
echo ======================================================
echo.
echo   URL:  http://127.0.0.1:8000
echo   Docs: http://127.0.0.1:8000/docs
echo.
echo   Press CTRL+C to stop the server.
echo.

"%PYTHON_EXE%" -m uvicorn api.server:app --host 0.0.0.0 --port 8000 --reload

pause
