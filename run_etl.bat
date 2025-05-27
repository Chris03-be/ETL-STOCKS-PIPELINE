@echo off
REM Activer l'environnement virtuel et lancer le pipeline ETL Python

REM Récupérer la date et l'heure au format ISO
for /f %%x in ('wmic os get localdatetime ^| findstr /b [0-9]') do set X=%%x
set CUR_DATE=%X:~0,4%-%X:~4,2%-%X:~6,2%
set CUR_TIME=%X:~8,2%:%X:~10,2%:%X:~12,2%

echo %CUR_DATE% %CUR_TIME% - Début du script >> run_etl.log
call "C:\Users\bedac\Desktop\Python ETL process\.venv\Scripts\activate"
python "C:\Users\bedac\Desktop\Python ETL process\etl_stocks.py" >> run_etl.log 2>&1
echo %CUR_DATE% %CUR_TIME% - Fin du script >> run_etl.log
echo. >> run_etl.log

pause
