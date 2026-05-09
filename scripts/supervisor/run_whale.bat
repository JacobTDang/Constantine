@echo off
REM Constantine — whale-tracker sidecar supervisor (Windows).
REM Restarts on crash with exponential backoff. No-ops gracefully if
REM data\whale_wallets.json is empty / missing.

setlocal enabledelayedexpansion
set BACKOFF=2
set MAX_BACKOFF=60

cd /d %~dp0..\..

:loop
echo [whale-supervisor] starting whale_tracker.py at %date% %time%
.venv\Scripts\python.exe scripts\whale_tracker.py --watch
set EXIT_CODE=!ERRORLEVEL!
echo [whale-supervisor] sidecar exited with code !EXIT_CODE! at %date% %time%

timeout /t !BACKOFF! /nobreak > nul
set /a BACKOFF=!BACKOFF! * 2
if !BACKOFF! gtr !MAX_BACKOFF! set BACKOFF=!MAX_BACKOFF!
goto loop
