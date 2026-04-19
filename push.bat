@echo off
cd /d %~dp0
git add .
git diff --staged --quiet && (echo Keine Aenderungen. & pause & exit /b 0)
git commit -m "update: %date% %time:~0,5%"
git push
echo.
echo Fertig - gepusht!
pause
