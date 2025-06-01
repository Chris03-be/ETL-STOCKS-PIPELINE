# Pipeline ETL Financier - Version Securisee avec Configuration Dotenv
![Automation](https://img.shields.io/badge/Automation-Scheduled-brightgreen?style=flat&logo=clockify&logoColor=white)

## Description

Pipeline ETL robuste et securise pour extraire des donnees financieres historiques de Yahoo Finance pour 30 tickers repartis en 5 secteurs principaux et 5 indices majeurs. Cette version integre une gestion complete des colonnes multi-indexees yfinance 2.51+, une configuration securisee via variables d'environnement, et un systeme de logging avance.

## Fonctionnalites Cles

### Securite et Configuration
- Configuration dotenv : Variables sensibles externalisees dans .env
- Validation automatique des variables d'environnement au demarrage
- Tests de connexion MySQL et yfinance avant traitement
- Logs securises avec masquage des mots de passe

### Robustesse Technique
- Gestion des colonnes multi-indexees yfinance 2.51+ avec aplatissement automatique
- Retry logic configurable avec 3 tentatives par ticker
- Validation et nettoyage des donnees avec verification des prix
- Conversion securisee des types numeriques et des dates
- Creation automatique des colonnes manquantes avec valeurs par defaut

### Monitoring et Logs
- Systeme de logging dual : fichier + console
- Logs horodates avec rotation automatique
- Statistiques detaillees par secteur et monitoring complet
- Metriques de performance en temps reel

## Architecture du Pipeline

### Configuration Securisee
```
.env → Variables d'environnement → Configuration validee → Pipeline securise
```

### Flux ETL Complet
```
Yahoo Finance → Extraction → Validation → Transformation → MySQL → Power BI
```

### Structure des Composants
- Configuration : Chargement et validation des variables .env
- Extraction : Telechargement avec gestion multi-index yfinance 2.51+
- Transformation : Nettoyage, validation et conversion des donnees
- Chargement : Insertion optimisee MySQL avec contraintes NOT NULL
- Monitoring : Logs detailles et statistiques de performance

## Installation et Configuration

### 1. Prerequis
```bash
# Installation des dependances
pip install pandas pymysql yfinance python-dotenv
```

### 2. Configuration de l'Environnement

Creer le fichier .env a la racine du projet :
```env
# CONFIGURATION BASE DE DONNEES
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=root
DB_PASSWORD=votre_mot_de_passe
DB_DATABASE=stock_data

# CONFIGURATION PIPELINE
PIPELINE_MAX_RETRIES=3
PIPELINE_SLEEP_BETWEEN_RETRIES=2

# CONFIGURATION YFINANCE
YF_TIMEOUT=30
YF_THREADS=True
YF_AUTO_ADJUST=True

# CONFIGURATION LOGS
LOG_DIRECTORY=logs
LOG_LEVEL=INFO
LOG_TO_FILE=True
LOG_TO_CONSOLE=True
```

### 3. Configuration MySQL
```sql
-- Creer la base de donnees
CREATE DATABASE stock_data;

-- Utiliser la base
USE stock_data;

-- La table sera creee automatiquement par le pipeline
```

### 4. Structure des Fichiers
```
pipeline_etl_financier/
├── etl_stocks.py          # Script principal
├── .env                   # Variables d'environnement (PRIVE)
├── .env.example          # Exemple de configuration (PUBLIC)
├── .gitignore            # Exclusions Git
├── logs/                 # Dossier des logs (cree automatiquement)
├── README.md             # Cette documentation
└── requirements.txt      # Dependances Python
```

## Utilisation

### Execution Standard
```bash
# Lancer le pipeline complet
python etl_stocks.py
```

### Tests de Configuration
Le pipeline effectue automatiquement :
- Validation des variables d'environnement
- Test de connexion MySQL
- Test de connexion yfinance
- Validation de la configuration pipeline

### Resultats Attendus
```
[DOTENV] Variables d'environnement chargees avec succes
[TEST] Tous les tests de configuration reussis
[PIPELINE] DEBUT - CONFIGURATION DOTENV VALIDEE
[SUCCESS] Succes: 30/30 (100.0%)
[ROWS] Total lignes: ~7,500
[READY] Base de donnees prete pour Power BI!
```

## ⚡ Automatisation et Planification

### Configuration du Planificateur de Taches Windows

#### 1. Ouvrir le Planificateur de Taches
```
Demarrer → Rechercher "Planificateur de taches" → Ouvrir
```

#### 2. Creer une Nouvelle Tache
```
Actions → Creer une tache de base...
Nom : "Pipeline ETL Financier"
Description : "Extraction quotidienne des donnees financieres"
```

#### 3. Configuration du Declencheur
```
Onglet "Declencheurs" → Nouveau...
Commencer la tache : "Selon une planification"
Parametres : Quotidien
Heure de debut : 06:00:00
Repeter chaque : 1 jour
```

#### 4. Configuration de l'Action
```
Onglet "Actions" → Nouveau...
Action : "Demarrer un programme"
Programme/script : C:\Python\python.exe
Ajouter des arguments : "C:\chemin\vers\etl_stocks.py"
Commencer dans : C:\chemin\vers\dossier\projet
```

#### 5. Parametres Avances
```
Onglet "Conditions" :
- Demarrer la tache seulement si l'ordinateur est sur secteur
- Reveiller l'ordinateur pour executer cette tache

Onglet "Parametres" :
- Autoriser l'execution de la tache a la demande
- Executer la tache des que possible apres un demarrage planifie manque
- Si la tache echoue, la redemarrer toutes les : 1 minute
- Tenter de redemarrer jusqu'a : 3 fois
```

### Script Batch pour Automatisation
Creer un fichier `run_etl.bat` :
```batch
@echo off
cd /d "C:\chemin\vers\votre\projet"
call .venv\Scripts\activate
python etl_stocks.py
pause
```

### Verification de l'Automatisation
```bash
# Verifier les logs d'execution
type logs\etl_pipeline_*.log | findstr "PIPELINE"

# Verifier la derniere execution
dir logs\ /od
```

### Monitoring Automatise
Le pipeline genere automatiquement :
- Logs horodates dans le dossier `logs/`
- Metriques de performance (duree, taux de succes)
- Alertes en cas d'echec de configuration
- Statistiques detaillees par secteur

## Donnees et Structure

### Tickers Traites (30 total)
- Indices (5) : S&P 500, NASDAQ, Dow Jones, FTSE 100, Nikkei 225
- Technologie (5) : AAPL, MSFT, GOOGL, META, NVDA
- Finance (5) : JPM, BAC, V, MA, GS
- Energie (5) : XOM, CVX, COP, SLB, EOG
- Industrie (5) : BA, CAT, GE, MMM, HON
- Sante (5) : JNJ, PFE, UNH, ABBV, MRK

### Table MySQL : historical_prices
```sql
CREATE TABLE historical_prices (
    date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    type VARCHAR(20) NOT NULL DEFAULT 'stock',
    sector VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    name VARCHAR(100) NOT NULL DEFAULT 'Unknown',
    country VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    continent VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    open FLOAT NOT NULL DEFAULT 0,
    high FLOAT NOT NULL DEFAULT 0,
    low FLOAT NOT NULL DEFAULT 0,
    close FLOAT NOT NULL DEFAULT 0,
    volume BIGINT NOT NULL DEFAULT 0,
    adj_close FLOAT NOT NULL DEFAULT 0,
    last_updated DATETIME NOT NULL,
    PRIMARY KEY (date, ticker),
    INDEX idx_sector_date (sector, date),
    INDEX idx_country (country),
    INDEX idx_type (type)
);
```

### Metadonnees Disponibles
- Temporelles : date, last_updated
- Identifiants : ticker, name, type
- Geographiques : country, continent
- Sectorielles : sector
- Financieres : open, high, low, close, volume, adj_close

## Connexion Power BI

### Parametres de Connexion
```
Type : MySQL
Serveur : localhost (ou 127.0.0.1)
Port : 3306
Base de donnees : stock_data
Table principale : historical_prices
```

### Analyses Recommandees
- Performance sectorielle : Evolution des cours par secteur
- Analyses temporelles : Tendances sur 12 mois glissants
- Volatilite : Calcul des ecarts-types et bandes de Bollinger
- Volume d'echanges : Analyse de la liquidite par instrument
- Comparaisons geographiques : Performance USA vs Europe vs Asie
- Correlations : Matrices de correlation entre secteurs

### KPIs Suggeres
- Rendement annuel par secteur
- Volatilite moyenne par region
- Volume moyen par type d'instrument
- Performance relative vs indices de reference
- Ratio risque/rendement par ticker

## Maintenance et Evolution

### Monitoring
- Logs detailles dans logs/etl_pipeline_YYYYMMDD_HHMMSS.log
- Metriques de performance : duree, taux de succes, lignes inserees
- Statistiques par secteur : suivi de la qualite des donnees
- Alertes automatiques en cas d'echec de configuration

### Securite
- Variables sensibles externalisees dans .env
- Fichier .env exclu du controle de version
- Mots de passe masques dans tous les logs
- Validation des entrees avant traitement

### Points d'Attention
- Evolution yfinance : Surveillance des changements d'API
- Tickers delistes : Maintenance periodique de la liste
- Qualite des donnees : Monitoring des valeurs anormales
- Performance : Optimisation pour plus de tickers si necessaire

### Evolutions Futures
- Support de nouveaux marches internationaux
- Integration d'autres sources de donnees financieres
- API REST pour acces programmatique aux donnees
- Dashboard de monitoring en temps reel

## Depannage

### Erreurs Communes

1. Fichier .env non trouve
```
ERREUR: Fichier .env non trouve ou mal configure!
```
Solution : Creer le fichier .env avec les variables requises

2. Erreur de connexion MySQL
```
[TEST] Connexion MySQL: X - Access denied
```
Solution : Verifier les identifiants dans .env

3. Erreur yfinance
```
[TEST] Connexion yfinance: ! - timeout
```
Solution : Verifier la connexion internet et augmenter YF_TIMEOUT

4. Erreur de planification Windows
```
La tache n'a pas pu demarrer
```
Solution : Verifier les chemins absolus dans la configuration de la tache

### Logs de Debug
```bash
# Consulter les logs detailles
tail -f logs/etl_pipeline_*.log

# Verifier la configuration
grep "CONFIG" logs/etl_pipeline_*.log

# Verifier les executions automatiques (Windows)
type logs\etl_pipeline_*.log | findstr "DEBUT"
```

## Support et Contribution

### Structure du Projet
- Version : 3.0 - Securisee avec Dotenv + Automatisation
- Compatibilite : yfinance 2.51+, Python 3.8+, Windows 10/11
- Statut : Production Ready

### Contribution
1. Fork du projet
2. Creer une branche feature
3. Tester avec votre configuration .env
4. Valider l'automatisation Windows
5. Soumettre une pull request

### Documentation Technique
- Configuration detaillee dans .env.example
- Logs structures pour debugging
- Tests automatiques de configuration
- Validation des donnees en temps reel
- Guide d'automatisation Windows

---

Pipeline ETL Financier - Pret pour l'Analyse BI Avancee Automatisee

Derniere mise a jour : Mai 2025

---
Answer from Perplexity: pplx.ai/share