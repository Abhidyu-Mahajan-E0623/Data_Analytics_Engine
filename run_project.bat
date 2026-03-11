@echo off
setlocal DisableDelayedExpansion
title ProcDNA Intelligence Core

echo =======================================================
echo   Starting ProcDNA Intelligence Core API Server
echo =======================================================
echo.

set "PROJECT_ROOT=%~dp0"
set "PYTHONPATH=%PROJECT_ROOT%"

:: Activate virtual environment if it exists
if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call ".venv\Scripts\activate.bat"
) else (
    echo [WARNING] No .venv found. Using global Python.
)

:: Wait 2 seconds, then open the browser (runs async)
start "" /B cmd /c "ping localhost -n 3 >nul & start http://localhost:8050"

echo.
echo Launching Uvicorn Server...
echo The dashboard will open automatically in your browser.
echo Press Ctrl+C to stop the server.
echo.

uvicorn api.server:app --host 127.0.0.1 --port 8050
