@echo off
cd /d "%~dp0"
echo ============================================================
echo  S^&P 500 Chartbook Generator
echo ============================================================
echo.
python generate_chartbook_direct.py
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python script failed.
    pause
    exit /b 1
)
echo.
echo Pushing updated data to GitHub...
git add chartbook_direct.html custom_portfolios.json
git commit -m "Update chartbook data %date% %time%"
git push origin master
echo.
echo Opening chartbook on GitHub Pages...
start "" "https://jsheth78.github.io/stock-price-vs-target/chartbook_direct.html"
echo Done.
