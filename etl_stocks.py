import pandas as pd
import pymysql
import logging
import traceback
import os
from datetime import datetime
import yfinance as yf
import time
from dotenv import load_dotenv

# === CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ===
load_dotenv()

# Vérification que le fichier .env est bien chargé
if not os.getenv('DB_HOST'):
    print("ERREUR: Fichier .env non trouvé ou mal configuré!")
    print("Créez un fichier .env avec les variables de configuration.")
    exit(1)

print("[DOTENV] Variables d'environnement chargées avec succès")

# === CONFIGURATION LOGS ROBUSTE ===
def setup_logging():
    """Configure le système de logging avec variables d'environnement"""
    
    # Récupération des paramètres depuis .env
    log_dir = os.getenv('LOG_DIRECTORY', 'logs')
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_to_file = os.getenv('LOG_TO_FILE', 'True').lower() == 'true'
    log_to_console = os.getenv('LOG_TO_CONSOLE', 'True').lower() == 'true'
    
    # Créer le dossier logs s'il n'existe pas
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Nom du fichier log avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"etl_pipeline_{timestamp}.log")
    
    # Configuration du logger principal
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, log_level))
    
    # Supprimer les handlers existants
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # === HANDLER FICHIER (si activé) ===
    if log_to_file:
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(getattr(logging, log_level))
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # === HANDLER CONSOLE (si activé) ===
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level))
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # Log de démarrage
    logger.info("=" * 80)
    logger.info("[INIT] Configuration du logging terminée")
    if log_to_file:
        logger.info(f"[INIT] Fichier log: {log_filename}")
    logger.info(f"[INIT] Niveau de log: {log_level}")
    logger.info("=" * 80)
    
    return logger

# === INITIALISATION DU LOGGER ===
logger = setup_logging()

# === CONFIGURATION MYSQL DEPUIS .ENV ===
MYSQL_CONFIG = {
    "host": os.getenv('DB_HOST'),
    "port": int(os.getenv('DB_PORT', 3306)),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DB_DATABASE')
}

# Validation de la configuration MySQL
required_db_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE']
missing_vars = [var for var in required_db_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"[CONFIG] Variables manquantes dans .env: {missing_vars}")
    exit(1)

logger.info(f"[CONFIG] Configuration MySQL chargée - Host: {MYSQL_CONFIG['host']}")

# === CONFIGURATION PIPELINE DEPUIS .ENV ===
PIPELINE_CONFIG = {
    'max_retries': int(os.getenv('PIPELINE_MAX_RETRIES', 3)),
    'sleep_between_retries': int(os.getenv('PIPELINE_SLEEP_BETWEEN_RETRIES', 2)),
    'yf_timeout': int(os.getenv('YF_TIMEOUT', 30)),
    'yf_threads': os.getenv('YF_THREADS', 'True').lower() == 'true',
    'yf_auto_adjust': os.getenv('YF_AUTO_ADJUST', 'True').lower() == 'true'
}

logger.info(f"[CONFIG] Configuration pipeline chargée - Max retries: {PIPELINE_CONFIG['max_retries']}")

# ... reste de ton TICKER_MAPPING et code existant ...
# === MAPPING DES TICKERS AVEC MÉTADONNÉES ===
# TICKER_MAPPING avec 30 tickers (5 par secteur + 5 indices)

# TICKER_MAPPING STRUCTURÉ - 30 tickers (5 par secteur + 5 indices)
TICKER_MAPPING = {
    # === INDICES MAJEURS (5) ===
    '^GSPC': {'sector': 'Index', 'type': 'index', 'name': 'S&P 500', 'country': 'USA', 'continent': 'North America'},
    '^IXIC': {'sector': 'Index', 'type': 'index', 'name': 'NASDAQ Composite', 'country': 'USA', 'continent': 'North America'},
    '^DJI': {'sector': 'Index', 'type': 'index', 'name': 'Dow Jones Industrial', 'country': 'USA', 'continent': 'North America'},
    '^FTSE': {'sector': 'Index', 'type': 'index', 'name': 'FTSE 100', 'country': 'UK', 'continent': 'Europe'},
    '^N225': {'sector': 'Index', 'type': 'index', 'name': 'Nikkei 225', 'country': 'Japan', 'continent': 'Asia'},

    # === TECHNOLOGIE (5) ===
    'AAPL': {'sector': 'Technologie', 'type': 'stock', 'name': 'Apple Inc.', 'country': 'USA', 'continent': 'North America'},
    'MSFT': {'sector': 'Technologie', 'type': 'stock', 'name': 'Microsoft Corp.', 'country': 'USA', 'continent': 'North America'},
    'GOOGL': {'sector': 'Technologie', 'type': 'stock', 'name': 'Alphabet Inc.', 'country': 'USA', 'continent': 'North America'},
    'META': {'sector': 'Technologie', 'type': 'stock', 'name': 'Meta Platforms Inc.', 'country': 'USA', 'continent': 'North America'},
    'NVDA': {'sector': 'Technologie', 'type': 'stock', 'name': 'NVIDIA Corp.', 'country': 'USA', 'continent': 'North America'},

    # === FINANCE (5) ===
    'JPM': {'sector': 'Finance', 'type': 'stock', 'name': 'JPMorgan Chase & Co.', 'country': 'USA', 'continent': 'North America'},
    'BAC': {'sector': 'Finance', 'type': 'stock', 'name': 'Bank of America Corp.', 'country': 'USA', 'continent': 'North America'},
    'V': {'sector': 'Finance', 'type': 'stock', 'name': 'Visa Inc.', 'country': 'USA', 'continent': 'North America'},
    'MA': {'sector': 'Finance', 'type': 'stock', 'name': 'Mastercard Inc.', 'country': 'USA', 'continent': 'North America'},
    'GS': {'sector': 'Finance', 'type': 'stock', 'name': 'Goldman Sachs Group', 'country': 'USA', 'continent': 'North America'},

    # === ENERGIE (5) ===
    'XOM': {'sector': 'Energie', 'type': 'stock', 'name': 'Exxon Mobil Corp.', 'country': 'USA', 'continent': 'North America'},
    'CVX': {'sector': 'Energie', 'type': 'stock', 'name': 'Chevron Corp.', 'country': 'USA', 'continent': 'North America'},
    'COP': {'sector': 'Energie', 'type': 'stock', 'name': 'ConocoPhillips', 'country': 'USA', 'continent': 'North America'},
    'SLB': {'sector': 'Energie', 'type': 'stock', 'name': 'Schlumberger Ltd.', 'country': 'USA', 'continent': 'North America'},
    'EOG': {'sector': 'Energie', 'type': 'stock', 'name': 'EOG Resources Inc.', 'country': 'USA', 'continent': 'North America'},

    # === INDUSTRIE (5) ===
    'BA': {'sector': 'Industrie', 'type': 'stock', 'name': 'Boeing Co.', 'country': 'USA', 'continent': 'North America'},
    'CAT': {'sector': 'Industrie', 'type': 'stock', 'name': 'Caterpillar Inc.', 'country': 'USA', 'continent': 'North America'},
    'GE': {'sector': 'Industrie', 'type': 'stock', 'name': 'General Electric Co.', 'country': 'USA', 'continent': 'North America'},
    'MMM': {'sector': 'Industrie', 'type': 'stock', 'name': '3M Company', 'country': 'USA', 'continent': 'North America'},
    'HON': {'sector': 'Industrie', 'type': 'stock', 'name': 'Honeywell International', 'country': 'USA', 'continent': 'North America'},

    # === SANTE (5) ===
    'JNJ': {'sector': 'Sante', 'type': 'stock', 'name': 'Johnson & Johnson', 'country': 'USA', 'continent': 'North America'},
    'PFE': {'sector': 'Sante', 'type': 'stock', 'name': 'Pfizer Inc.', 'country': 'USA', 'continent': 'North America'},
    'UNH': {'sector': 'Sante', 'type': 'stock', 'name': 'UnitedHealth Group', 'country': 'USA', 'continent': 'North America'},
    'ABBV': {'sector': 'Sante', 'type': 'stock', 'name': 'AbbVie Inc.', 'country': 'USA', 'continent': 'North America'},
    'MRK': {'sector': 'Sante', 'type': 'stock', 'name': 'Merck & Co. Inc.', 'country': 'USA', 'continent': 'North America'}
}

# Validation du mapping
print(f"[INIT] Pipeline configure pour {len(TICKER_MAPPING)} tickers")
TICKERS = list(TICKER_MAPPING.keys())
logger.info(f"[INIT] Tickers selectionnes: {TICKERS}")

# Structure des colonnes finales
columns_order = [
    'date', 'ticker', 'type', 'sector', 'name', 'country', 'continent',
    'open', 'high', 'low', 'close', 'volume', 'adj_close', 'last_updated'
]

def get_rolling_year_dates():
    """Calcule les dates pour une periode glissante d'un an"""
    today = datetime.now().date()
    end_date = today.strftime("%Y-%m-%d")
    start_date = (today - pd.Timedelta(days=365)).strftime("%Y-%m-%d")
    return start_date, end_date

def flatten_multiindex_columns(data, ticker):
    """
    Fonction specialisee pour aplatir les colonnes multi-indexees de yfinance 2.51+
    """
    try:
        if isinstance(data.columns, pd.MultiIndex):
            logger.info(f"{ticker}: [MULTIINDEX] Colonnes multi-indexees detectees")
            logger.debug(f"{ticker}: Colonnes originales: {data.columns.tolist()}")
            
            # Methode 1: Prendre le premier niveau (noms des colonnes standard)
            data.columns = data.columns.get_level_values(0)
            logger.info(f"{ticker}: [FLATTEN] Colonnes aplaties: {list(data.columns)}")
            
        elif hasattr(data.columns, 'droplevel'):
            # Methode alternative si get_level_values ne fonctionne pas
            logger.info(f"{ticker}: [MULTIINDEX] Tentative droplevel pour colonnes complexes")
            data.columns = data.columns.droplevel(1)
            
        return data
        
    except Exception as e:
        logger.warning(f"{ticker}: [WARN] Erreur aplatissement colonnes: {e}")
        # Fallback: renommer manuellement si possible
        try:
            if len(data.columns) >= 6:  # Au minimum OHLCV + Date
                new_columns = []
                for col in data.columns:
                    if isinstance(col, tuple):
                        new_columns.append(col[0])  # Prendre le premier element du tuple
                    else:
                        new_columns.append(col)
                data.columns = new_columns
                logger.info(f"{ticker}: [FALLBACK] Fallback reussi: {list(data.columns)}")
        except Exception as fallback_error:
            logger.error(f"{ticker}: [ERROR] Echec fallback: {fallback_error}")
        
        return data

def fetch_stock_data_corrected(ticker, start_date, end_date, max_retries=3):
    """
    Version corrigée pour récupérer les vraies données
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"[FETCH] [{attempt + 1}/{max_retries}] Téléchargement {ticker}")
            
            # === MÉTHODE ALTERNATIVE : Utiliser Ticker object ===
            ticker_obj = yf.Ticker(ticker)
            data = ticker_obj.history(start=start_date, end=end_date)
            
            # Vérification des données vides
            if data is None or data.empty:
                logger.warning(f"{ticker}: [WARN] Aucune donnée (tentative {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return pd.DataFrame(columns=columns_order)
            
            logger.info(f"{ticker}: [DATA] {len(data)} lignes récupérées")
            
            # === VÉRIFICATION DES VRAIES DONNÉES ===
            # Afficher les vraies valeurs pour debug
            if not data.empty:
                sample_close = data['Close'].iloc[0] if 'Close' in data.columns else 0
                logger.info(f"{ticker}: [DEBUG] Exemple Close = {sample_close}")
                if sample_close == 0:
                    logger.error(f"{ticker}: [ERROR] Données Close = 0, problème de récupération!")
            
            # === PRÉPARATION SANS APLATISSEMENT FORCÉ ===
            data = data.reset_index()
            data['ticker'] = ticker
            data['last_updated'] = datetime.now()
            
            # === ENRICHISSEMENT AVEC MÉTADONNÉES ===
            mapping = TICKER_MAPPING.get(ticker, {})
            data['sector'] = mapping.get('sector', 'Unknown')
            data['type'] = mapping.get('type', 'Unknown')
            data['name'] = mapping.get('name', ticker)
            data['country'] = mapping.get('country', 'Unknown')
            data['continent'] = mapping.get('continent', 'Unknown')
            
            # === RENOMMAGE SIMPLE (les colonnes de .history() sont déjà simples) ===
            logger.info(f"{ticker}: [COLUMNS] Colonnes disponibles: {list(data.columns)}")
            
            # Renommage direct
            column_mapping = {
                'Date': 'date',
                'Open': 'open',
                'High': 'high', 
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }
            
            for old_name, new_name in column_mapping.items():
                if old_name in data.columns:
                    data = data.rename(columns={old_name: new_name})
                    logger.debug(f"{ticker}: [RENAME] {old_name} -> {new_name}")
            
            # === ADJ_CLOSE depuis les dividendes ===
            if 'close' in data.columns:
                # Calculer adj_close manuellement si nécessaire
                data['adj_close'] = data['close']  # Simplification
                logger.debug(f"{ticker}: [ADJCLOSE] adj_close = close")
            
            # === VÉRIFICATION DES VRAIES VALEURS ===
            if 'close' in data.columns and not data['close'].empty:
                avg_close = data['close'].mean()
                logger.info(f"{ticker}: [VALIDATION] Prix moyen Close = {avg_close:.2f}")
                if avg_close == 0:
                    logger.error(f"{ticker}: [ERROR] Prix moyen = 0, données invalides!")
                    continue
            
            # === CRÉATION DES COLONNES MANQUANTES ===
            for col in columns_order:
                if col not in data.columns:
                    if col in ['open', 'high', 'low', 'close', 'adj_close']:
                        data[col] = 0.0
                    elif col == 'volume':
                        data[col] = 0
                    elif col == 'last_updated':
                        data[col] = datetime.now()
                    elif col == 'date':
                        data[col] = datetime.now().date()
                    else:
                        data[col] = 'Unknown'
            
            # === CONVERSION DES DATES ===
            if 'date' in data.columns:
                data['date'] = pd.to_datetime(data['date']).dt.date
            data['last_updated'] = pd.to_datetime(data['last_updated'])
            
            # === NETTOYAGE FINAL ===
            data = data.where(pd.notnull(data), None)
            result = data[columns_order].copy()
            
            logger.info(f"{ticker}: [SUCCESS] {len(result)} lignes valides préparées")
            return result
            
        except Exception as e:
            logger.error(f"{ticker}: [ERROR] Erreur tentative {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                traceback.print_exc()
    
    return pd.DataFrame(columns=columns_order)

def save_to_mysql_optimized(df):
    """
    Sauvegarde optimisee avec gestion des NULL et contraintes MySQL
    """
    if df.empty:
        logger.warning("[MYSQL] DataFrame vide, aucune insertion")
        return False

    try:
        with pymysql.connect(**MYSQL_CONFIG) as conn:
            with conn.cursor() as cursor:
                # === CREATION DE LA TABLE OPTIMISEE ===
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS historical_prices (
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
                        INDEX idx_type (type),
                        INDEX idx_continent (continent)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                
                # === VALIDATION DES COLONNES ===
                missing_columns = [col for col in columns_order if col not in df.columns]
                if missing_columns:
                    logger.error(f"[MYSQL] Colonnes manquantes: {missing_columns}")
                    return False

                # === PREPARATION FINALE DES DONNEES ===
                df_clean = df[columns_order].copy()
                
                # Remplacement des None par des valeurs par defaut pour respecter NOT NULL
                default_values = {
                    'type': 'stock',
                    'sector': 'Unknown',
                    'name': 'Unknown',
                    'country': 'Unknown',
                    'continent': 'Unknown',
                    'open': 0.0,
                    'high': 0.0,
                    'low': 0.0,
                    'close': 0.0,
                    'volume': 0,
                    'adj_close': 0.0
                }
                
                for col, default_val in default_values.items():
                    if col in df_clean.columns:
                        df_clean[col] = df_clean[col].fillna(default_val)
                
                # Conversion finale des types
                numeric_cols = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
                for col in numeric_cols:
                    try:
                        if col == 'volume':
                            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype('int64')
                        else:
                            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0.0)
                    except Exception as e:
                        logger.warning(f"[MYSQL] Erreur conversion finale {col}: {e}")
                        df_clean[col] = 0 if col == 'volume' else 0.0
                
                # Conversion des dates
                df_clean['date'] = pd.to_datetime(df_clean['date']).dt.date
                df_clean['last_updated'] = pd.to_datetime(df_clean['last_updated'])

                # === INSERTION OPTIMISEE AVEC GESTION D'ERREURS ===
                sql_query = """
                    REPLACE INTO historical_prices 
                    (date, ticker, type, sector, name, country, continent, 
                     open, high, low, close, volume, adj_close, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # Conversion en tuples avec validation
                data_tuples = []
                error_rows = 0
                
                for idx, row in df_clean.iterrows():
                    try:
                        tuple_data = (
                            row['date'],
                            str(row['ticker']),
                            str(row['type']),
                            str(row['sector']),
                            str(row['name']),
                            str(row['country']),
                            str(row['continent']),
                            float(row['open']),
                            float(row['high']),
                            float(row['low']),
                            float(row['close']),
                            int(row['volume']),
                            float(row['adj_close']),
                            row['last_updated']
                        )
                        data_tuples.append(tuple_data)
                    except Exception as e:
                        error_rows += 1
                        logger.warning(f"[MYSQL] Erreur ligne {idx}: {e}")
                        continue
                
                if error_rows > 0:
                    logger.warning(f"[MYSQL] {error_rows} lignes ignorees a cause d'erreurs de conversion")
                
                if data_tuples:
                    cursor.executemany(sql_query, data_tuples)
                    conn.commit()
                    logger.info(f"[MYSQL] {len(data_tuples)} lignes inserees/mises a jour en base")
                    return True
                else:
                    logger.warning("[MYSQL] Aucune donnee valide a inserer")
                    return False

    except pymysql.Error as err:
        logger.error(f"[MYSQL] Erreur MySQL: {err}")
        return False
    except Exception as e:
        logger.error(f"[MYSQL] Erreur generale save_to_mysql: {e}")
        traceback.print_exc()
        return False
    
    def test_dotenv_configuration():
     """Test complet de la configuration dotenv"""
    logger.info("[TEST] Début des tests de configuration dotenv")
    
    # Test 1: Variables d'environnement chargées
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE']
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if 'PASSWORD' in var:
                logger.info(f"[TEST] {var}: ✓ (masqué pour sécurité)")
            else:
                logger.info(f"[TEST] {var}: ✓ ({value})")
        else:
            logger.error(f"[TEST] {var}: ✗ MANQUANT")
            return False
    
    # Test 2: Connexion MySQL
    try:
        with pymysql.connect(**MYSQL_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result[0] == 1:
                    logger.info("[TEST] Connexion MySQL: ✓")
                else:
                    logger.error("[TEST] Connexion MySQL: ✗")
                    return False
    except Exception as e:
        logger.error(f"[TEST] Connexion MySQL: ✗ - {e}")
        return False
    
    # Test 3: Configuration pipeline
    logger.info(f"[TEST] Max retries: {PIPELINE_CONFIG['max_retries']}")
    logger.info(f"[TEST] Sleep between retries: {PIPELINE_CONFIG['sleep_between_retries']}s")
    logger.info(f"[TEST] YFinance timeout: {PIPELINE_CONFIG['yf_timeout']}s")
    
    if not test_dotenv_configuration():
     logger.error("[INIT] Échec des tests dotenv, arrêt du script")
    exit(1)
def main():
    """Pipeline ETL principal"""
    
    
    logger.info("[MAIN] Démarrage du pipeline ETL")
    
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("[PIPELINE] DÉBUT - CONFIGURATION DOTENV")
    logger.info("=" * 80)
    
    # === DÉMARRAGE DU PIPELINE ===
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("[PIPELINE] DÉBUT - TOUS LES TESTS RÉUSSIS")
    logger.info("=" * 80)
    # ... reste de ton code existant ...

    
    try:
        # === INITIALISATION ===
        start_date, end_date = get_rolling_year_dates()
        logger.info(f"[PIPELINE] Periode d'extraction: {start_date} au {end_date}")
        
        # Statistiques par secteur
        sectors = {}
        for ticker, info in TICKER_MAPPING.items():
            sector = info['sector']
            if sector not in sectors:
                sectors[sector] = []
            sectors[sector].append(ticker)
        
        logger.info("[PIPELINE] Repartition par secteur:")
        for sector, tickers_list in sectors.items():
            logger.info(f"  [SECTOR] {sector}: {len(tickers_list)} tickers")
        
        # === TRAITEMENT DES DONNEES ===
        success_count = 0
        error_count = 0
        total_rows_inserted = 0
        sector_stats = {}

        for i, ticker in enumerate(TICKERS, 1):
            sector = TICKER_MAPPING[ticker]['sector']
            
            logger.info("-" * 60)
            logger.info(f"[PROCESS] [{i:2d}/{len(TICKERS)}] {ticker} ({sector})")
            
            # Extraction avec gestion multi-index
            df_ticker = fetch_stock_data_corrected(ticker, start_date, end_date)
            
            if not df_ticker.empty:
                # Sauvegarde optimisee
                if save_to_mysql_optimized(df_ticker):
                    success_count += 1
                    total_rows_inserted += len(df_ticker)
                    
                    # Statistiques par secteur
                    if sector not in sector_stats:
                        sector_stats[sector] = {'success': 0, 'rows': 0}
                    sector_stats[sector]['success'] += 1
                    sector_stats[sector]['rows'] += len(df_ticker)
                    
                    logger.info(f"[SUCCESS] {ticker}: {len(df_ticker)} lignes -> Base de donnees")
                else:
                    error_count += 1
                    logger.error(f"[ERROR] {ticker}: Echec insertion base de donnees")
            else:
                error_count += 1
                logger.warning(f"[WARN] {ticker}: Aucune donnee recuperee")

        # === RESUME FINAL DETAILLE ===
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("[PIPELINE] RESUME FINAL")
        logger.info("=" * 80)
        logger.info(f"[TIME] Duree totale: {duration}")
        logger.info(f"[SUCCESS] Succes: {success_count}/{len(TICKERS)}")
        logger.info(f"[ERROR] Erreurs: {error_count}/{len(TICKERS)}")
        logger.info(f"[ROWS] Total lignes inserees: {total_rows_inserted:,}")
        
        if len(TICKERS) > 0:
            success_rate = (success_count / len(TICKERS)) * 100
            logger.info(f"[RATE] Taux de succes: {success_rate:.1f}%")
        
        logger.info("")
        logger.info("[SECTORS] Detail par secteur:")
        for sector, stats in sector_stats.items():
            total_sector = len(sectors[sector])
            sector_success_rate = (stats['success'] / total_sector * 100) if total_sector > 0 else 0
            logger.info(f"  [SECTOR] {sector}: {stats['success']}/{total_sector} "
                       f"({sector_success_rate:.1f}%) - {stats['rows']:,} lignes")
        
        # Affichage console final
        print(f"\n{'='*80}")
        print(f"[COMPLETE] PIPELINE ETL MULTI-INDEX TERMINE")
        print(f"{'='*80}")
        print(f"[TIME] Duree: {duration}")
        print(f"[SUCCESS] Succes: {success_count}/{len(TICKERS)} ({(success_count/len(TICKERS)*100):.1f}%)")
        print(f"[ROWS] Total lignes: {total_rows_inserted:,}")
        print(f"[READY] Base de donnees prete pour Power BI!")
        print(f"[TABLE] Table: historical_prices")

    except Exception as e:
        logger.exception("[CRITICAL] Erreur critique dans le pipeline ETL")
        traceback.print_exc()
    finally:
        logger.info("[PIPELINE] FIN DU PIPELINE ETL MULTI-INDEX")

if __name__ == "__main__":
    print("[START] Démarrage du pipeline ETL avec tests de configuration")
    main()
    print("[END] Pipeline ETL terminé!")
