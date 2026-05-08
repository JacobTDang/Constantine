@echo off
REM Constantine — NBA projections sidecar supervisor (Windows).
REM Restarts the Python sidecar on crash with exponential backoff.
REM
REM Run alongside run_bot.bat (different terminal / Task Scheduler entry).

setlocal enabledelayedexpansion
set BACKOFF=2
set MAX_BACKOFF=60

cd /d %~dp0..\..

:loop
echo [sidecar-supervisor] starting nba_projections.py at %date% %time%
.venv\Scripts\python.exe scripts\nba_projections.py --watch
set EXIT_CODE=!ERRORLEVEL!
echo [sidecar-supervisor] sidecar exited with code !EXIT_CODE! at %date% %time%

timeout /t !BACKOFF! /nobreak > nul
set /a BACKOFF=!BACKOFF! * 2
if !BACKOFF! gtr !MAX_BACKOFF! set BACKOFF=!MAX_BACKOFF!
goto loop
