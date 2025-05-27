from dotenv import load_dotenv # type: ignore
load_dotenv()
import os

import os # Gardé au cas où, mais pas pour la config MySQL
import yfinance as yf # type: ignore
import pandas as pd # type: ignore
import pymysql # type: ignore # On reste sur PyMySQL
from datetime import datetime, timedelta

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("Test INFO")
logger.warning("Test WARNING")
logger.error("Test ERROR")

def get_period_dates(days=365):
    today = datetime.now().date()
    start_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    return start_date, end_date

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
    "cursorclass": pymysql.cursors.DictCursor  # Si tu utilises PyMySQL
}

for key in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]:
    if not os.getenv(key):
        print(f"ERREUR : la variable {key} n'est pas définie dans .env !")
        exit()

# Mapping des tickers par secteur et type
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

TICKERS = list(TICKER_MAPPING.keys())
logger.info(f"Pipeline configuré pour {len(TICKERS)} tickers")

from datetime import datetime, timedelta

def get_rolling_year_dates():
    """
    Retourne la période glissante d'un an (365 jours) jusqu'à aujourd'hui
    """
    today = datetime.now().date()
    end_date = today.strftime("%Y-%m-%d")  # aujourd'hui
    start_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")  # il y a 365 jours
    return start_date, end_date

def main():
    logger.info("Début du pipeline ETL")
    success_count = 0
    error_count = 0
    try:
        # Calcul de la période
        start_date, end_date = get_rolling_year_dates()
        logger.info(f"Extraction des données du {start_date} au {end_date}")
        print(f"Période analysée : {start_date} au {end_date}")
        # Ici, vous pouvez ajouter la logique de traitement pour incrémenter success_count et error_count si besoin
        logger.info(f"Pipeline terminé - Succès: {success_count}, Erreurs: {error_count}")
        
    except Exception as e:
        logger.exception("Erreur critique dans le pipeline ETL")
    finally:
        logger.info("Fin du pipeline ETL")

def fetch_stock_data(ticker, start_date, end_date):
    try:
        logger.info(f"Extraction de {ticker}")
        
        # Téléchargement avec paramètres optimisés
        data = yf.download(
            ticker, 
            start=start_date, 
            end=end_date, 
            progress=False, 
            auto_adjust=True, 
            actions=False
        )
        
        if data.empty:
            logger.warning(f"Aucune donnée disponible pour {ticker}")
            return pd.DataFrame()
        
        data.reset_index(inplace=True)
        
        # Gestion des colonnes multi-indexées
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] if col[0] != '' else 'Date' for col in data.columns.values]
        
        # Standardisation des noms de colonnes
        column_mapping = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adj_close',
            'Volume': 'volume'
        }
        
        data = data.rename(columns=column_mapping)
        
        # Ajout des métadonnées
        ticker_info = TICKER_MAPPING.get(ticker, {'sector': 'Unknown', 'type': 'stock'})
        data['ticker'] = ticker
        data['sector'] = ticker_info['sector']
        data['type'] = ticker_info['type']
        data['last_updated'] = datetime.now()
        
        # Gestion de adj_close si absent
        if 'adj_close' not in data.columns:
            data['adj_close'] = data['close']
        
        # Nettoyage des données
        numeric_columns = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
        data[numeric_columns] = data[numeric_columns].fillna(0)
        data = data.dropna(subset=['date', 'ticker'])
        
        logger.info(f"✅ {ticker}: {len(data)} lignes extraites")
        return data
        
    except Exception as e:
        logger.error(f"❌ Erreur pour {ticker}: {e}")
        return pd.DataFrame()

def save_to_mysql(df):
    if df.empty:
        logger.warning("Aucune donnée à insérer (DataFrame vide).")
        return

    try:
        with pymysql.connect(**MYSQL_CONFIG) as conn:
            with conn.cursor() as cursor:
                # Création de table avec types cohérents
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS historical_prices (
                        date DATE NOT NULL,
                        ticker VARCHAR(20) NOT NULL,
                        open FLOAT NOT NULL,
                        high FLOAT NOT NULL,
                        low FLOAT NOT NULL,
                        close FLOAT NOT NULL,
                        volume BIGINT NOT NULL,
                        last_updated DATETIME NOT NULL,
                        type VARCHAR(20),
                        sector VARCHAR(50),
                        adj_close FLOAT,
                        PRIMARY KEY (date, ticker)
                    )
                """)
                
                # GARANTIR L'ORDRE DES COLONNES
                columns_order = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'last_updated', 'type', 'sector', 'adj_close']
                df_ordered = df[columns_order]
                
                sql_query = """
                    REPLACE INTO historical_prices (date, ticker, open, high, low, close, volume, last_updated, type, sector, adj_close)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                data_tuples = [tuple(x) for x in df_ordered.to_numpy()]
                
                if data_tuples:
                    cursor.executemany(sql_query, data_tuples)
                    conn.commit()
                    logger.info(f"→ {len(df)} lignes insérées/actualisées dans MySQL")
                else:
                    logger.warning("Aucune donnée à insérer après conversion en tuples")
                    
    except pymysql.Error as err:
        logger.error(f"Erreur PyMySQL: {err}")
    except Exception as e:
        logger.exception(f"Erreur générale dans save_to_mysql: {e}")

if __name__ == "__main__":
    print("INFO: Exécution avec configuration MySQL en dur pour test (sans .env).")
    # LA VÉRIFICATION os.getenv A ÉTÉ SUPPRIMÉE ICI POUR CE TEST
    # # if not all(os.getenv(key) for key in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]):
    # #     print("ERREUR: Variables d'environnement MySQL manquantes. Vérifiez votre fichier .env et l'appel à load_dotenv().")
    # #     exit()

    start_date, end_date = get_rolling_year_dates()
    print(f"Période analysée : {start_date} au {end_date}")

    print("\nPipeline terminé.") # Message légèrement ajusté
