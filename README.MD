ETL-STOCKS-PIPELINE
![Automatisé avec Task Scheduler](https://img.shields.ioshields.io/badge/)
PROJET REALISER AVEC CLAUDE 4.0 SONNET

Ce projet est un pipeline ETL automatisé qui extrait quotidiennement les données boursières de 60 tickers mondiaux via Yahoo Finance, les transforme avec Python/pandas et les charge dans une base de données MySQL structurée.

Le but est de fournir une base de données à jour pour l'analyse BI, la visualisation Power BI et la prise de décision business basée sur des données financières mondiales.

🎯 Fonctionnalités
Pipeline ETL Complet
✅ Extraction automatisée de 60 tickers mondiaux (indices, finance, industrie, tech, énergie)

✅ Transformation et nettoyage des données avec pandas

✅ Chargement sécurisé dans MySQL avec gestion d'erreurs

✅ Logging professionnel pour traçabilité et debugging

Automatisation & Sécurité
✅ Planification automatique avec le Planificateur de tâches Windows

✅ Sécurisation des identifiants via fichier .env

✅ Gestion d'erreurs robuste avec retry automatique

✅ Extensibilité facile (ajout de nouveaux tickers)

Analyse BI Ready
✅ Structure optimisée pour Power BI, SQL et DAX

✅ Classification sectorielle (Finance, Tech, Énergie, Industrie, Indices)

✅ Données géographiques (USA, Europe, Asie, Canada)

✅ KPIs pré-calculés (variations, volumes, dates de mise à jour)

📈 Données Couvertes
60 Tickers Répartis en 5 Catégories
Secteur	Nombre	Exemples
Indices Mondiaux	12	S&P 500 (^GSPC), NASDAQ (^IXIC), CAC 40 (^FCHI), Nikkei (^N225)
Finance	12	JPMorgan (JPM), Visa (V), HSBC (HSBC), Mastercard (MA)
Industrie	12	Boeing (BA), Caterpillar (CAT), Airbus (AIR.PA), Toyota (TM)
Technologie	12	Apple (AAPL), Microsoft (MSFT), Nvidia (NVDA), Google (GOOGL)
Énergie	12	ExxonMobil (XOM), Shell (SHEL), TotalEnergies (TOT.PA), Tesla (TSLA)
Couverture Géographique Mondiale
🇺🇸 États-Unis - 🇪🇺 Europe - 🇬🇧 Royaume-Uni - 🇨🇳 Chine/Hong Kong - 🇯🇵 Japon - 🇨🇦 Canada - 🇦🇺 Australie - 🇮🇳 Inde

🛠️ Prérequis
Python 3.8+

MySQL Server 8.0+

Windows (pour l'automatisation avec Task Scheduler)

Connexion Internet (pour Yahoo Finance API)

Power BI Desktop (optionnel, pour l'analyse)

⚡ Installation
1. Cloner le dépôt
bash
git clone https://github.com/Chris03-be/ETL-STOCKS-PIPELINE.git
cd ETL-STOCKS-PIPELINE
2. Créer et activer l'environnement virtuel
bash
python -m venv .venv
.venv\Scripts\activate   # Windows
# ou
source .venv/bin/activate   # Mac/Linux
3. Installer les dépendances
bash
pip install -r requirements.txt
4. Configurer la base MySQL
sql
-- Créer la base de données
CREATE DATABASE IF NOT EXISTS stock_data;

-- Créer un utilisateur dédié (optionnel)
CREATE USER IF NOT EXISTS 'etl_user'@'localhost' IDENTIFIED BY 'mot_de_passe_securise';
GRANT ALL PRIVILEGES ON stock_data.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
5. Configurer le fichier .env
bash
# Copier le fichier d'exemple
cp .env.example .env
Puis remplir avec vos identifiants MySQL :

text
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=etl_user
MYSQL_PASSWORD=votre_mot_de_passe
MYSQL_DATABASE=stock_data
🚀 Utilisation
Exécution Manuelle
bash
python etl_stocks.py
Automatisation (Planificateur de tâches Windows)
Utiliser le fichier batch run_etl.bat :

text
@echo off
cd "C:\chemin\vers\ETL-STOCKS-PIPELINE"
.venv\Scripts\activate
python etl_stocks.py
Configurer le Planificateur de tâches :

Ouvrir "Planificateur de tâches Windows"

Créer une tâche de base

Programmer l'exécution (ex: quotidienne à 8h00)

Action : Démarrer le programme run_etl.bat

📊 Utilisation pour l'Analyse BI (Power BI, SQL, DAX)
Ce projet ETL prépare une base de données MySQL structurée et à jour, idéale pour l'analyse avancée et la création de dashboards.

1. Connexion à la Base MySQL
Ouvre Power BI Desktop ou ton outil d'analyse préféré

Connecte-toi à la base MySQL créée par le pipeline

Sélectionne la table historical_prices

2. Préparation des Données avec SQL
Utilise des requêtes SQL pour filtrer, agréger ou transformer les données avant import dans Power BI.

Exemple - Performance par secteur :

sql
SELECT 
    sector,
    ticker,
    date,
    close,
    (close - open) / open * 100 AS daily_variation
FROM historical_prices
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY sector, date DESC;
Exemple - Top performers :

sql
SELECT 
    ticker,
    sector,
    AVG((close - open) / open * 100) as avg_daily_return,
    STDDEV((close - open) / open * 100) as volatility
FROM historical_prices 
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY ticker, sector
ORDER BY avg_daily_return DESC;
3. Création de Mesures et KPIs avec DAX (dans Power BI)
Dans Power BI, crée des mesures dynamiques pour enrichir tes analyses.

Exemples de mesures DAX :

text
// Variation journalière
Variation Journalière = 
DIVIDE([Close] - [Open], [Open], 0) * 100

// Moyenne mobile 30 jours
Moyenne Mobile 30J = 
CALCULATE(
    AVERAGE(historical_prices[close]),
    DATESINPERIOD(historical_prices[date], LASTDATE(historical_prices[date]), -30, DAY)
)

// Volume moyen par secteur
Volume Moyen Secteur = 
CALCULATE(
    AVERAGE(historical_prices[volume]),
    ALLEXCEPT(historical_prices, historical_prices[sector])
)

// Performance YTD
Performance YTD = 
VAR FirstPrice = CALCULATE(FIRSTNONBLANK(historical_prices[close], 1), DATESYTD(historical_prices[date]))
VAR LastPrice = CALCULATE(LASTNONBLANK(historical_prices[close], 1), DATESYTD(historical_prices[date]))
RETURN DIVIDE(LastPrice - FirstPrice, FirstPrice, 0) * 100
4. Construction du Dashboard
Visuels recommandés : Line charts (tendances), Bar charts (comparaisons sectorielles), Cards (KPIs), Tables (détails)

Filtres dynamiques : Date, Secteur, Type (action/indice), Ticker

Storytelling : Focus sur les insights actionnables et la clarté des tendances

5. Bonnes Pratiques BI
✅ Rafraîchissement automatique : Configure Power BI Service pour synchroniser avec MySQL

✅ Modèle en étoile : Utilise les colonnes sector, type, date comme dimensions

✅ Performance : Crée des index MySQL sur les colonnes fréquemment filtrées

✅ Gouvernance : Ne modifie pas le pipeline si tu veux garder une base stable

🔧 Ajouter d'Autres Entreprises (Tickers)
Pour suivre de nouvelles actions, ajoutez simplement leur ticker dans le dictionnaire TICKER_MAPPING du script etl_stocks.py :

python
TICKER_MAPPING = {
    # Ajouter de nouveaux tickers ici
    'NFLX': {'sector': 'Technologie', 'type': 'stock'},
    'DIS': {'sector': 'Industrie', 'type': 'stock'},
    # ...
}
Le script détectera automatiquement les nouveaux tickers et les intégrera au pipeline.

📁 Structure du Projet
text
/ETL-STOCKS-PIPELINE/
├── etl_stocks.py           # Script principal du pipeline ETL
├── .env                    # Configuration MySQL (à créer)
├── .env.example           # Exemple de configuration
├── .gitignore             # Fichiers à ignorer par Git
├── requirements.txt       # Dépendances Python
├── run_etl.bat           # Script batch pour Windows
├── pipeline.log          # Logs d'exécution (généré automatiquement)
├── README.md             # Documentation du projet
└── .venv/                # Environnement virtuel Python
🛠️ Technologies Utilisées
Catégorie	Technologies
Backend	Python 3.8+, pandas, yfinance, PyMySQL
Base de Données	MySQL 8.0+
Sécurité	python-dotenv
Automatisation	Windows Task Scheduler
Analyse BI	Power BI, SQL, DAX
Monitoring	Logging Python
📊 Structure des Données MySQL
Table : historical_prices
Colonne	Type	Description
date	DATE	Date de la cotation (PK)
ticker	VARCHAR(20)	Symbole du ticker (PK)
open	FLOAT	Prix d'ouverture
high	FLOAT	Prix le plus haut
low	FLOAT	Prix le plus bas
close	FLOAT	Prix de clôture
volume	BIGINT	Volume d'échange
adj_close	FLOAT	Prix ajusté (dividendes/splits)
sector	VARCHAR(50)	Secteur (Finance, Technologie, etc.)
type	VARCHAR(20)	Type (stock, index)
last_updated	DATETIME	Date de dernière mise à jour
🚨 Dépannage
Erreurs Communes
Erreur	Solution
NameError: 'get_rolling_year_dates' not defined	Vérifier que toutes les fonctions sont définies dans le script
Unknown column 'sector'	Exécuter ALTER TABLE historical_prices ADD COLUMN sector VARCHAR(50)
Connection refused	Vérifier que MySQL est démarré
Timeout yfinance	Relancer le script, vérifier la connexion Internet
Logs et Monitoring
bash
# Vérifier les dernières exécutions
tail -f pipeline.log

# Compter les erreurs récentes
grep "ERROR" pipeline.log | tail -10
🤝 Contribution
Fork le projet

Créer une branche feature (git checkout -b feature/nouvelle-fonctionnalite)

Commit les changements (git commit -am 'Ajout nouvelle fonctionnalité')

Push vers la branche (git push origin feature/nouvelle-fonctionnalite)

Créer une Pull Request

👨‍💻 Auteur
Chris Beda

🐙 GitHub : Chris03-be

💼 etudiant en Business Intelligence et analyse de données

pipelines ETL et visualisation Power BI

📄 Licence
MIT License - Voir le fichier LICENSE pour plus de détails.

⭐ Si ce projet vous a été utile pour vos analyses BI, n'hésitez pas à lui donner une étoile !

