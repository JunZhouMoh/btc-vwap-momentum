@echo off
REM BTC Trading Bot Startup Script

cd /d "%~dp0"

REM Activate virtual environment and run the bot
call ..\..\.venv\Scripts\activate.bat
python main.py

pause
