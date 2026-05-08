@echo off
REM Constantine — autonomous supervisor (Windows).
REM Restarts the bot binary on crash with exponential backoff capped at 60s.
REM
REM Usage:
REM   scripts\supervisor\run_bot.bat
REM
REM Add to Task Scheduler with "Run whether user is logged on or not" +
REM "Run with highest privileges" + "Trigger: at startup" for AFK operation.

setlocal enabledelayedexpansion
set BACKOFF=2
set MAX_BACKOFF=60

cd /d %~dp0..\..

:loop
echo [supervisor] starting polymarket-bot at %date% %time%
target\release\polymarket-bot.exe
set EXIT_CODE=!ERRORLEVEL!
echo [supervisor] bot exited with code !EXIT_CODE! at %date% %time%

REM Sleep BACKOFF seconds, then double up to MAX_BACKOFF
timeout /t !BACKOFF! /nobreak > nul
set /a BACKOFF=!BACKOFF! * 2
if !BACKOFF! gtr !MAX_BACKOFF! set BACKOFF=!MAX_BACKOFF!
goto loop
