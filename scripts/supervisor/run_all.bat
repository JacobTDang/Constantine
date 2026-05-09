@echo off
REM Constantine — launch bot + all 3 sidecars in separate windows.
REM Each window has its own supervisor (auto-restart on crash).
REM Close any window to halt that component.

cd /d %~dp0..\..

start "Constantine Bot" cmd /k scripts\supervisor\run_bot.bat
start "NBA Projections" cmd /k scripts\supervisor\run_sidecar.bat
start "Sportsbook Devig" cmd /k scripts\supervisor\run_devig.bat
start "Whale Tracker" cmd /k scripts\supervisor\run_whale.bat

echo.
echo Launched 4 windows: bot + 3 sidecars.
echo Logs are inside each window. Close a window to halt that component.
echo Soak data lands in data\db\signals.jsonl and data\db\positions.jsonl.
echo.
