@echo off
REM Constantine — sportsbook devig sidecar supervisor (Windows).
REM Restarts on crash with exponential backoff. No-ops gracefully if
REM THE_ODDS_API_KEY is not set in .env.

setlocal enabledelayedexpansion
set BACKOFF=2
set MAX_BACKOFF=60

cd /d %~dp0..\..

:loop
echo [devig-supervisor] starting sportsbook_devig.py at %date% %time%
.venv\Scripts\python.exe scripts\sportsbook_devig.py --watch
set EXIT_CODE=!ERRORLEVEL!
echo [devig-supervisor] sidecar exited with code !EXIT_CODE! at %date% %time%

timeout /t !BACKOFF! /nobreak > nul
set /a BACKOFF=!BACKOFF! * 2
if !BACKOFF! gtr !MAX_BACKOFF! set BACKOFF=!MAX_BACKOFF!
goto loop
