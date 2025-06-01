@echo off
REM ====================================================================
REM Pipeline ETL Financier - Script d'Automatisation Windows
REM Version: 3.0 - Production Ready
REM ====================================================================

REM Configuration des chemins (MODIFIE SELON TON ENVIRONNEMENT)
set PROJECT_DIR=C:\Users\%USERNAME%\Desktop\Python ETL process
set PYTHON_VENV=%PROJECT_DIR%\.venv\Scripts
set ETL_SCRIPT=%PROJECT_DIR%\etl_stocks.py

REM Affichage de l'en-tête
echo ====================================================================
echo Pipeline ETL Financier - Execution Automatique
echo Heure de debut: %date% %time%
echo ====================================================================

REM Changement vers le répertoire du projet
echo [INFO] Changement vers le repertoire du projet...
cd /d "%PROJECT_DIR%"
if %errorlevel% neq 0 (
    echo [ERREUR] Impossible d'acceder au repertoire: %PROJECT_DIR%
    echo Verifiez que le chemin existe et est accessible.
    pause
    exit /b 1
)

REM Vérification de l'existence de l'environnement virtuel
echo [INFO] Verification de l'environnement virtuel...
if not exist "%PYTHON_VENV%\python.exe" (
    echo [ERREUR] Environnement virtuel Python introuvable
    echo Chemin recherche: %PYTHON_VENV%\python.exe
    echo.
    echo Solutions possibles:
    echo 1. Creer l'environnement virtuel: python -m venv .venv
    echo 2. Verifier le chemin dans le script .bat
    pause
    exit /b 1
)

REM Vérification du script ETL
echo [INFO] Verification du script ETL...
if not exist "%ETL_SCRIPT%" (
    echo [ERREUR] Script ETL introuvable: %ETL_SCRIPT%
    echo Verifiez que le fichier etl_stocks.py existe.
    pause
    exit /b 1
)

REM Vérification du fichier .env
echo [INFO] Verification de la configuration...
if not exist "%PROJECT_DIR%\.env" (
    echo [ERREUR] Fichier .env introuvable
    echo Le fichier de configuration .env est requis.
    echo Copiez .env.example vers .env et configurez vos parametres.
    pause
    exit /b 1
)

REM Activation de l'environnement virtuel
echo [INFO] Activation de l'environnement virtuel Python...
call "%PYTHON_VENV%\activate.bat"
if %errorlevel% neq 0 (
    echo [ERREUR] Echec de l'activation de l'environnement virtuel
    pause
    exit /b 1
)

REM Affichage de la version Python (pour vérification)
echo [INFO] Version Python utilisee:
python --version

REM Création du dossier logs s'il n'existe pas
if not exist "%PROJECT_DIR%\logs" (
    echo [INFO] Creation du dossier logs...
    mkdir "%PROJECT_DIR%\logs"
)

REM Exécution du pipeline ETL
echo ====================================================================
echo [INFO] Lancement du Pipeline ETL Financier...
echo ====================================================================
echo.

python "%ETL_SCRIPT%"
set ETL_EXIT_CODE=%errorlevel%

echo.
echo ====================================================================

REM Vérification du résultat d'exécution
if %ETL_EXIT_CODE% equ 0 (
    echo [SUCCES] Pipeline ETL execute avec succes!
    echo Heure de fin: %date% %time%
    echo.
    echo Resultats:
    echo - Donnees extraites de Yahoo Finance
    echo - Donnees transformees et validees  
    echo - Donnees inserees dans MySQL
    echo - Logs detailles disponibles dans: logs\
    echo.
    echo Base de donnees prete pour Power BI!
) else (
    echo [ECHEC] Le Pipeline ETL a echoue
    echo Code d'erreur: %ETL_EXIT_CODE%
    echo.
    echo Actions recommandees:
    echo 1. Consultez les logs dans le dossier logs\
    echo 2. Verifiez la connexion MySQL
    echo 3. Verifiez la connexion Internet pour Yahoo Finance
    echo 4. Verifiez le fichier .env
)

REM Désactivation de l'environnement virtuel
echo [INFO] Desactivation de l'environnement virtuel...
call "%PYTHON_VENV%\deactivate.bat" 2>nul

echo ====================================================================
echo Execution terminee: %date% %time%
echo ====================================================================

REM Pause seulement en cas d'erreur (pour debugging)
if %ETL_EXIT_CODE% neq 0 (
    echo.
    echo Appuyez sur une touche pour fermer cette fenetre...
    pause >nul
)

REM Retour du code d'erreur pour Task Scheduler
exit /b %ETL_EXIT_CODE%
