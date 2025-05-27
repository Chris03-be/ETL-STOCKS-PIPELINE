import os
import yfinance as yf
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import logging
import traceback

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_rolling_year_dates():
    """Retourne la période glissante d'un an jusqu'à aujourd'hui."""
    today = datetime.now().date()
    end_date = today.strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    return start_date, end_date

# Configuration MySQL
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "Bigmoneymaker888",
    "database": "stock_data",
    "cursorclass": pymysql.cursors.DictCursor
}

def test_mysql_connection():
    try:
        with pymysql.connect(**MYSQL_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()
                print(f" Connexion MySQL réussie : {version}")
                logger.info("Connexion MySQL validée")
                return True
    except Exception as e:
        print(f" Erreur de connexion MySQL : {e}")
        logger.error(f"Erreur MySQL : {e}")
        return False

if not test_mysql_connection():
    print("Impossible de continuer sans connexion MySQL")
    exit(1)

logger.info("Configuration MySQL chargée avec succès")

# --- Mapping des tickers par secteur et type (repris du fichier paste.txt) ---
# --- Mapping des tickers par secteur et type ---
TICKER_MAPPING = {
    # Indices
    '^GSPC': {'sector': 'Index', 'type': 'index'},
    '^IXIC': {'sector': 'Index', 'type': 'index'},
    '^DJI': {'sector': 'Index', 'type': 'index'},
    '^FTSE': {'sector': 'Index', 'type': 'index'},
    '^N225': {'sector': 'Index', 'type': 'index'},
    'GDAXI': {'sector': 'Index', 'type': 'index'},
    '^FCHI': {'sector': 'Index', 'type': 'index'},
    '^HSI': {'sector': 'Index', 'type': 'index'},
    '^STOXX50E': {'sector': 'Index', 'type': 'index'},
    '^SSEC': {'sector': 'Index', 'type': 'index'},
    '^BSESN': {'sector': 'Index', 'type': 'index'},
    '^AXJO': {'sector': 'Index', 'type': 'index'},

    # Finance
    'JPM': {'sector': 'Finance', 'type': 'stock'},
    'BAC': {'sector': 'Finance', 'type': 'stock'},
    'WFC': {'sector': 'Finance', 'type': 'stock'},
    'C': {'sector': 'Finance', 'type': 'stock'},
    'GS': {'sector': 'Finance', 'type': 'stock'},
    'MS': {'sector': 'Finance', 'type': 'stock'},
    'V': {'sector': 'Finance', 'type': 'stock'},
    'MA': {'sector': 'Finance', 'type': 'stock'},
    'AXP': {'sector': 'Finance', 'type': 'stock'},
    'HSBC': {'sector': 'Finance', 'type': 'stock'},
    'RY': {'sector': 'Finance', 'type': 'stock'},
    '1398.HK': {'sector': 'Finance', 'type': 'stock'},

    # Industrie
    'BA': {'sector': 'Industrie', 'type': 'stock'},
    'CAT': {'sector': 'Industrie', 'type': 'stock'},
    'GE': {'sector': 'Industrie', 'type': 'stock'},
    'MMM': {'sector': 'Industrie', 'type': 'stock'},
    'HON': {'sector': 'Industrie', 'type': 'stock'},
    'DE': {'sector': 'Industrie', 'type': 'stock'},
    'SI': {'sector': 'Industrie', 'type': 'stock'},
    'ROG.SW': {'sector': 'Industrie', 'type': 'stock'},
    'SIE.DE': {'sector': 'Industrie', 'type': 'stock'},
    'AIR.PA': {'sector': 'Industrie', 'type': 'stock'},
    'NSANY': {'sector': 'Industrie', 'type': 'stock'},
    'TM': {'sector': 'Industrie', 'type': 'stock'},

    # Technologie
    'AAPL': {'sector': 'Technologie', 'type': 'stock'},
    'MSFT': {'sector': 'Technologie', 'type': 'stock'},
    'NVDA': {'sector': 'Technologie', 'type': 'stock'},
    'GOOGL': {'sector': 'Technologie', 'type': 'stock'},
    'META': {'sector': 'Technologie', 'type': 'stock'},
    'TSLA': {'sector': 'Technologie', 'type': 'stock'},
    'BABA': {'sector': 'Technologie', 'type': 'stock'},
    'TCEHY': {'sector': 'Technologie', 'type': 'stock'},
    'SAP.DE': {'sector': 'Technologie', 'type': 'stock'},
    'ASML.AS': {'sector': 'Technologie', 'type': 'stock'},
    'ORCL': {'sector': 'Technologie', 'type': 'stock'},
    'IBM': {'sector': 'Technologie', 'type': 'stock'},

    # Énergie
    'XOM': {'sector': 'Energie', 'type': 'stock'},
    'CVX': {'sector': 'Energie', 'type': 'stock'},
    'SHEL': {'sector': 'Energie', 'type': 'stock'},
    'BP': {'sector': 'Energie', 'type': 'stock'},
    'TOT.PA': {'sector': 'Energie', 'type': 'stock'},
    'NEE': {'sector': 'Energie', 'type': 'stock'},
    'FSLR': {'sector': 'Energie', 'type': 'stock'},
    'VWS.CO': {'sector': 'Energie', 'type': 'stock'},
    'BEP': {'sector': 'Energie', 'type': 'stock'},
    '0916.HK': {'sector': 'Energie', 'type': 'stock'},
    'ENPH': {'sector': 'Energie', 'type': 'stock'},
    'PLUG': {'sector': 'Energie', 'type': 'stock'}
}

# Génération de la liste des tickers à traiter
TICKERS = list(TICKER_MAPPING.keys())
logger.info(f"Pipeline configuré pour {len(TICKERS)} tickers : {TICKERS}")

def fetch_stock_data(ticker, start_date, end_date):
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        data.reset_index(inplace=True)
        data['ticker'] = ticker
        data['last_updated'] = datetime.now()

        # Renommage pour correspondre à la table MySQL
        data = data.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'last_updated']]

        # Supprimer les lignes contenant des valeurs nulles
        data = data.dropna()
        if data.empty:
            print(f"→ Aucune donnée valide pour {ticker}.")
        return data
    except Exception as e:
        print(f"Erreur Yahoo Finance pour {ticker}: {str(e)}")
        return pd.DataFrame()

def save_to_mysql(df):
    if df.empty:
        logger.warning("Aucune donnée à insérer (DataFrame vide).")
        return

    try:
        with pymysql.connect(**MYSQL_CONFIG) as conn:
            with conn.cursor() as cursor:
                # Création de la table si elle n'existe pas
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS historical_prices (
                        date DATE NOT NULL,
                        ticker VARCHAR(20) NOT NULL,
                        open FLOAT,
                        high FLOAT,
                        low FLOAT,
                        close FLOAT,
                        volume BIGINT,
                        last_updated DATETIME,
                        type VARCHAR(20),
                        sector VARCHAR(50),
                        adj_close FLOAT,
                        PRIMARY KEY (date, ticker)
                    )
                """)
                columns_order = [
                    'date', 'ticker', 'open', 'high', 'low', 'close', 'volume',
                    'last_updated', 'type', 'sector', 'adj_close'
                ]
                missing_columns = [col for col in columns_order if col not in df.columns]
                if missing_columns:
                    logger.error(f"Colonnes manquantes dans le DataFrame: {missing_columns}")
                    return

                df_ordered = df[columns_order].copy()
                # Conversion et nettoyage des types
                df_ordered['date'] = pd.to_datetime(df_ordered['date']).dt.date
                df_ordered['last_updated'] = pd.to_datetime(df_ordered['last_updated'])
                for col in ['open', 'high', 'low', 'close', 'adj_close']:
                    df_ordered[col] = pd.to_numeric(df_ordered[col], errors='coerce').fillna(0)
                df_ordered['volume'] = pd.to_numeric(df_ordered['volume'], errors='coerce').fillna(0).astype('int64')
                # Remplacer les NaN restants par None (pour MySQL)
                df_ordered = df_ordered.where(pd.notnull(df_ordered), None)

                sql_query = """
                    REPLACE INTO historical_prices 
                    (date, ticker, open, high, low, close, volume, last_updated, type, sector, adj_close)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                data_tuples = [tuple(row) for row in df_ordered.to_numpy()]
                if data_tuples:
                    cursor.executemany(sql_query, data_tuples)
                    conn.commit()
                    logger.info(f"→ {len(df_ordered)} lignes insérées/actualisées dans MySQL")
                else:
                    logger.warning("Aucune donnée à insérer après conversion en tuples")

    except pymysql.Error as err:
        logger.error(f"Erreur PyMySQL: {err}")
        traceback.print_exc()
    except Exception as e:
        logger.exception(f"Erreur générale dans save_to_mysql: {e}")
        traceback.print_exc()

def main():
    logger.info("Début du pipeline ETL")
    try:
        start_date, end_date = get_rolling_year_dates()
        logger.info(f"Extraction des données du {start_date} au {end_date}")
        print(f"Période analysée : {start_date} au {end_date}")

        success_count = 0
        error_count = 0

        for ticker in TICKERS:
            try:
                logger.info(f"Traitement de {ticker}...")
                df_ticker = fetch_stock_data(ticker, start_date, end_date)
                if not df_ticker.empty:
                    save_to_mysql(df_ticker)
                    logger.info(f" {ticker} : {len(df_ticker)} lignes insérées")
                    success_count += 1
                else:
                    logger.warning(f"{ticker} : Aucune donnée récupérée")
                    error_count += 1
            except Exception as ticker_error:
                logger.error(f"Erreur pour {ticker}: {ticker_error}")
                traceback.print_exc()
                error_count += 1

        logger.info(f"Pipeline terminé - Succès: {success_count}, Erreurs: {error_count}")
        print(f"\n=== RÉSUMÉ FINAL ===")
        print(f"Succès: {success_count}, Erreurs: {error_count}")
        print(f"Total traité: {success_count + error_count} tickers")

    except Exception as e:
        logger.exception("Erreur critique dans le pipeline ETL")
        traceback.print_exc()
    finally:
        logger.info("Fin du pipeline ETL")

if __name__ == "__main__":
    print("INFO: Démarrage du pipeline ETL complet - 60 tickers")
    main()
    print("INFO: Pipeline ETL terminé")