@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%" || (echo [ERROR] Failed to enter project directory.& call :pause_if_needed & exit /b 1)

set "VENV_DIR=.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "PIP_EXE=%VENV_DIR%\Scripts\pip.exe"
set "REQ_FILE=requirements.txt"
set "REQ_HASH_FILE=%VENV_DIR%\requirements.sha256"
set "INTERACTIVE=0"

if not exist "%REQ_FILE%" (
  call :die "requirements.txt not found in %ROOT_DIR%" 1
)

set "BASE_PYTHON="
where py >nul 2>&1 && set "BASE_PYTHON=py -3"
if not defined BASE_PYTHON (
  where python >nul 2>&1 && set "BASE_PYTHON=python"
)
if not defined BASE_PYTHON (
  call :die "Python is not installed or not available in PATH." 1
)

if not exist "%PYTHON_EXE%" (
  echo [INFO] Creating virtual environment in %VENV_DIR%...
  %BASE_PYTHON% -m venv "%VENV_DIR%"
  if errorlevel 1 (
    call :die "Failed to create virtual environment." 1
  )
)

for /f %%H in ('powershell -NoProfile -Command "(Get-FileHash -Algorithm SHA256 -Path '%REQ_FILE%').Hash.ToLower()"') do set "REQ_HASH=%%H"
if not defined REQ_HASH (
  call :die "Failed to compute requirements hash." 1
)

set "INSTALL_DEPS=1"
if exist "%REQ_HASH_FILE%" (
  set /p STORED_HASH=<"%REQ_HASH_FILE%"
  if /I "!STORED_HASH!"=="!REQ_HASH!" set "INSTALL_DEPS=0"
)

if "!INSTALL_DEPS!"=="1" (
  echo [INFO] Installing/updating dependencies from %REQ_FILE%...
  "%PYTHON_EXE%" -m pip install --upgrade pip
  if errorlevel 1 (
    call :die "Failed to upgrade pip." 1
  )
  "%PIP_EXE%" install -r "%REQ_FILE%"
  if errorlevel 1 (
    call :die "Failed to install dependencies." 1
  )
  > "%REQ_HASH_FILE%" echo !REQ_HASH!
) else (
  echo [INFO] Dependencies already up to date. Skipping install.
)

if not "%~1"=="" (
  set "CLI_ARGS=%*"
  goto :run_cli
)

set "INTERACTIVE=1"
set "DEFAULT_DOMAIN=silver"
if exist ".env" (
  for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
    set "K=%%A"
    set "V=%%B"
    if defined K (
      if /I not "!K:~0,1!"=="#" (
        if /I "!K!"=="DATABRICKS_SCHEMA_DOMAIN" (
          if not "%%B"=="" set "DEFAULT_DOMAIN=%%B"
        )
      )
    )
  )
)
set "DEFAULT_DOMAIN=!DEFAULT_DOMAIN:"=!"
echo.
echo ======================================================
echo          What would you like to run?
echo ======================================================
echo.
echo   1. Hypothesis Generation
echo   2. Anomaly Detection
echo   3. Insight Generation
echo.
set /p CHOICE=Enter your choice [1, 2, or 3]: 
echo.

if "!CHOICE!"=="1" goto :menu_hypothesis
if "!CHOICE!"=="2" goto :menu_anomaly
if "!CHOICE!"=="3" goto :menu_insight
echo [ERROR] Invalid choice. Please enter 1, 2, or 3.
call :pause_if_needed
exit /b 1

:menu_hypothesis
set /p DOMAIN=Enter domain [default: !DEFAULT_DOMAIN!]: 
if not defined DOMAIN set "DOMAIN=!DEFAULT_DOMAIN!"
set "DOMAIN=!DOMAIN:"=!"
set "DEFAULT_FOCUS=!DOMAIN!"
set /p FOCUS_AREAS=Focus areas comma-separated [default: !DEFAULT_FOCUS!]: 
if not defined FOCUS_AREAS set "FOCUS_AREAS=!DEFAULT_FOCUS!"
set "FOCUS_AREAS=!FOCUS_AREAS:"=!"
set "FOCUS_AREAS=!FOCUS_AREAS: =!"
set "CLI_ARGS=generate --domain !DOMAIN! --focus !FOCUS_AREAS!"
goto :run_cli

:menu_anomaly
set /p SCHEMA=Enter schema to scan [default: bronze]: 
if not defined SCHEMA set "SCHEMA=bronze"
set "SCHEMA=!SCHEMA:"=!"
set "CLI_ARGS=anomaly-detect --schema !SCHEMA!"
goto :run_cli

:menu_insight
set /p RUN_ID=Enter run_id [leave blank for latest]: 
set "INSIGHT_ARGS=generate-insights"
if defined RUN_ID (
  set "RUN_ID=!RUN_ID:"=!"
  set "INSIGHT_ARGS=!INSIGHT_ARGS! --run-id !RUN_ID!"
)
set /p HYPO_NUMS=Enter hypothesis numbers comma-separated [e.g. 1,4,5,6]: 
if defined HYPO_NUMS (
  set "HYPO_NUMS=!HYPO_NUMS:"=!"
  set "INSIGHT_ARGS=!INSIGHT_ARGS! --hypotheses !HYPO_NUMS!"
)
set "CLI_ARGS=!INSIGHT_ARGS!"
goto :run_cli

:run_cli

echo [INFO] Running: python -m src.cli %CLI_ARGS%
"%PYTHON_EXE%" -m src.cli %CLI_ARGS%
set "RC=%ERRORLEVEL%"

if "%INTERACTIVE%"=="1" (
  echo.
  if "%RC%"=="0" (
    echo [INFO] Completed successfully.
  ) else (
    echo [ERROR] Command failed with exit code %RC%.
  )
  call :pause_if_needed
)

exit /b %RC%

:die
echo [ERROR] %~1
call :pause_if_needed
exit /b %~2

:pause_if_needed
if /I "%NO_PAUSE%"=="1" exit /b 0
pause
exit /b 0
