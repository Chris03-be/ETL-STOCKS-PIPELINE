# ETL-STOCKS-PIPELINE

![Automatisé avec Task Scheduler](https://img.shields.io/badge/Automatisation-Windows%20Task%20Scheduler-blue)

## Description

Ce projet est un pipeline ETL automatisé qui extrait quotidiennement les données boursières de plusieurs entreprises via Yahoo Finance, les transforme avec Python/pandas et les charge dans une base de données MySQL.  
Le but est de fournir une base de données à jour pour l’analyse, la visualisation (ex : Power BI) et la prise de décision business.

---

## Fonctionnalités

- Extraction automatisée des données pour plusieurs tickers (actions)
- Nettoyage et transformation des données avec pandas
- Chargement sécurisé dans MySQL
- Planification automatique avec le Planificateur de tâches Windows
- Sécurisation des identifiants via un fichier `.env`
- Facilement extensible à d’autres entreprises (ajout de tickers dans le script)

---

## Prérequis

- Python 3.x
- MySQL Server
- Un environnement virtuel Python (recommandé)
- Un compte Yahoo Finance (pour les tickers publics)
- Windows (pour l’automatisation avec Task Scheduler)

---

## Installation

1. **Cloner le dépôt**
    ```
    git clone https://github.com/Chris03-be/ETL-STOCKS-PIPELINE.git
    cd ETL-STOCKS-PIPELINE
    ```

2. **Créer et activer l’environnement virtuel**
    ```
    python -m venv .venv
    .venv\Scripts\activate   # Windows
    # ou
    source .venv/bin/activate   # Mac/Linux
    ```

3. **Installer les dépendances**
    ```
    pip install -r requirements.txt
    ```

4. **Configurer la base MySQL**
    - Créer la base et l’utilisateur si besoin.
    - Noter les identifiants.

5. **Configurer le fichier `.env`**
    - Copier `.env.example` en `.env`
    - Remplir avec vos identifiants MySQL

---

## Utilisation

### **Exécution manuelle**

python etl_stocks.py

text

### **Automatisation (Planificateur de tâches Windows)**

- Utilisez le fichier `run_etl.bat` pour lancer automatiquement le pipeline à la fréquence de votre choix.
- Voir la documentation ou le wiki du projet pour les instructions détaillées.

---

## Ajouter d’autres entreprises (tickers)

Pour suivre de nouvelles actions, ajoutez simplement leur ticker dans la liste `TICKERS` du script `etl_stocks.py` :

TICKERS = [
"NVDA", "MSFT", "GOOGL", "AMZN", "TSM", "LLY", "ENPH", "PLTR", "HD", "CRWD", "AAPL", "META"
]

text

---

## Structure du projet

/ETL-STOCKS-PIPELINE/
├── etl_stocks.py
├── .env
├── .env.example
├── .gitignore
├── requirements.txt
├── run_etl.bat
├── README.md
└── .venv/

text

---

## Technologies utilisées

- Python 3.x
- pandas
- yfinance
- PyMySQL
- python-dotenv
- MySQL
- Power BI (pour la visualisation)

---

## Auteur

Chris Beda  
[GitHub - Chris03-be](https://github.com/Chris03-be)

---

## Licence

MIT

---
