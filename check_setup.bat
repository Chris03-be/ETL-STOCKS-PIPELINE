@echo off
REM Script de verification de l'environnement

echo ====================================================================
echo Verification de l'Environnement Pipeline ETL
echo ====================================================================

set PROJECT_DIR=C:\Users\%USERNAME%\Desktop\Python ETL process
set ERROR_COUNT=0

echo Repertoire du projet: %PROJECT_DIR%
echo.

REM Test 1: Répertoire projet
if exist "%PROJECT_DIR%" (
    echo [OK] Repertoire projet trouve
) else (
    echo [ERREUR] Repertoire projet introuvable
    set /a ERROR_COUNT+=1
)

REM Test 2: Environnement virtuel Python
if exist "%PROJECT_DIR%\.venv\Scripts\python.exe" (
    echo [OK] Environnement virtuel Python trouve
    
    REM Affichage de la version
    cd /d "%PROJECT_DIR%"
    call .venv\Scripts\activate.bat >nul 2>&1
    for /f "tokens=*" %%i in ('python --version 2^>^&1') do echo     Version: %%i
    call .venv\Scripts\deactivate.bat >nul 2>&1
) else (
    echo [ERREUR] Environnement virtuel Python introuvable
    echo     Creez-le avec: python -m venv .venv
    set /a ERROR_COUNT+=1
)

REM Test 3: Script ETL principal
if exist "%PROJECT_DIR%\etl_stocks.py" (
    echo [OK] Script ETL principal trouve
) else (
    echo [ERREUR] Script etl_stocks.py introuvable
    set /a ERROR_COUNT+=1
)

REM Test 4: Fichier de configuration
if exist "%PROJECT_DIR%\.env" (
    echo [OK] Fichier de configuration .env trouve
) else (
    echo [ATTENTION] Fichier .env introuvable
    echo     Copiez .env.example vers .env et configurez-le
    set /a ERROR_COUNT+=1
)

REM Test 5: Dossier logs
if exist "%PROJECT_DIR%\logs" (
    echo [OK] Dossier logs existe
) else (
    echo [INFO] Creation du dossier logs...
    mkdir "%PROJECT_DIR%\logs" 2>nul
    if exist "%PROJECT_DIR%\logs" (
        echo [OK] Dossier logs cree
    ) else (
        echo [ERREUR] Impossible de creer le dossier logs
        set /a ERROR_COUNT+=1
    )
)

REM Test 6: Fichier batch principal
if exist "%PROJECT_DIR%\run_etl_pipeline.bat" (
    echo [OK] Script batch d'automatisation trouve
) else (
    echo [ATTENTION] Script run_etl_pipeline.bat introuvable
    echo     Ce fichier est necessaire pour l'automatisation
    set /a ERROR_COUNT+=1
)

echo.
echo ====================================================================
if %ERROR_COUNT% equ 0 (
    echo [SUCCES] Environnement pret pour l'automatisation!
    echo.
    echo Prochaines etapes:
    echo 1. Testez avec: test_etl_manual.bat
    echo 2. Configurez Task Scheduler avec run_etl_pipeline.bat
    echo 3. Planifiez l'execution quotidienne
) else (
    echo [ECHEC] %ERROR_COUNT% probleme(s) detecte(s)
    echo.
    echo Corrigez les erreurs avant de configurer l'automatisation.
)
echo ====================================================================

pause
echo Fin de la verification.