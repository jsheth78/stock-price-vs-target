@echo off
cd /d "%~dp0"
echo ============================================================
echo  S^&P 500 Chartbook Generator
echo ============================================================
echo.
python generate_chartbook_direct.py
echo.
echo Opening chartbook in browser...
start "" "chartbook_direct.html"
echo Done.
echo.
pause
