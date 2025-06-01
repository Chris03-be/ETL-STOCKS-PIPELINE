@echo off
REM Script de test manuel du pipeline ETL

echo ====================================================================
echo Test Manuel du Pipeline ETL Financier
echo ====================================================================

REM Demande de confirmation
set /p confirm="Voulez-vous executer le pipeline ETL maintenant? (o/n): "
if /i not "%confirm%"=="o" (
    echo Test annule par l'utilisateur.
    pause
    exit /b 0
)

REM Exécution du pipeline avec pause forcée
call run_etl_pipeline.bat

echo.
echo ====================================================================
echo Test termine. Consultez les resultats ci-dessus.
echo ====================================================================
pause
echo Fin du test manuel.
echo ====================================================================