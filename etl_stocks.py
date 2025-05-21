from dotenv import load_dotenv
load_dotenv()
import os


import os # Gardé au cas où, mais pas pour la config MySQL
import yfinance as yf
import pandas as pd
import pymysql # On reste sur PyMySQL
from datetime import datetime, timedelta

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


TICKERS = [
    "NVDA", "MSFT", "GOOGL", "AMZN",
    "TSM", "LLY", "ENPH", "PLTR", "HD", "CRWD"
]

def get_rolling_year_dates():
    today = datetime.now().date()
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=366)).strftime("%Y-%m-%d")
    return start_date, end_date

def fetch_stock_data(ticker, start_date, end_date):
    try:
        print(f"--- Début fetch_stock_data pour {ticker} ---")
        data = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True, actions=False)
        # auto_adjust=True simplifie souvent les colonnes (donne 'Adj Close' comme 'Close')
        # actions=False pour ne pas avoir les dividendes/splits si non désirés
        
        data.reset_index(inplace=True)
        print(f"Colonnes APRÈS yf.download et reset_index pour {ticker}: {data.columns.tolist()}")

        # Si les colonnes sont multi-indexées (ce qui est le cas ici)
        if isinstance(data.columns, pd.MultiIndex):
            print(f"Colonnes multi-indexées détectées pour {ticker}. Aplatissement...")
            # Aplatissement plus robuste :
            # Pour ('Date', ''), on veut 'Date'. Pour ('Close', 'NVDA'), on veut 'Close'.
            new_cols = []
            for col in data.columns.values:
                if isinstance(col, tuple):
                    if col[0] == 'Date' and col[1] == '': # Cas spécifique pour la date
                        new_cols.append('Date') # On la garde comme 'Date' (avec D majuscule pour l'instant)
                    else:
                        new_cols.append(col[0]) # On prend le premier niveau (Open, High, etc.)
                else:
                    new_cols.append(col) # Si ce n'est pas un tuple (peu probable ici après yf.download)
            data.columns = new_cols
            print(f"Colonnes APRÈS aplatissement pour {ticker}: {data.columns.tolist()}")

        # Maintenant, les colonnes devraient être simples: ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        # (ou similaires, 'Adj Close' peut être là si auto_adjust=False)

        # Renommage pour tout mettre en minuscules et standardiser
        rename_map_standard = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close', # Sera 'Adj Close' si auto_adjust=False et que tu veux le close ajusté
            'Adj Close': 'adj_close', # Si tu veux garder 'Adj Close' séparément
            'Volume': 'volume'
        }
        
        # Appliquer le renommage uniquement pour les colonnes présentes
        actual_rename_map = {k: v for k, v in rename_map_standard.items() if k in data.columns}
        data = data.rename(columns=actual_rename_map)
        print(f"Colonnes APRÈS renommage pour {ticker}: {data.columns.tolist()}")

        data['ticker'] = ticker
        data['last_updated'] = datetime.now()

        # Sélection finale des colonnes
        final_columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'last_updated']
        # Si tu as renommé 'Adj Close' en 'adj_close' et que tu veux l'utiliser à la place de 'close':
        # final_columns = ['date', 'ticker', 'open', 'high', 'low', 'adj_close', 'volume', 'last_updated']
        # Et alors, ta table MySQL devrait aussi avoir 'adj_close' au lieu de 'close'.
        # Pour l'instant, on assume que tu veux 'Close' (qui est déjà ajusté si auto_adjust=True)

        missing_cols = [col for col in final_columns if col not in data.columns]
        if missing_cols:
            print(f"ERREUR pour {ticker}: Colonnes finales manquantes après traitement: {missing_cols}")
            print(f"Colonnes disponibles dans data : {data.columns.tolist()}")
            raise KeyError(f"Colonnes finales manquantes : {missing_cols}")

        data = data[final_columns]

        colonnes_numeriques = ['open', 'high', 'low', 'close', 'volume'] # Exclut adj_close si tu ne l'utilises pas
        data[colonnes_numeriques] = data[colonnes_numeriques].fillna(0)
        data['last_updated'] = data['last_updated'].fillna(datetime.now())
        data = data.dropna(subset=['date', 'ticker'])

        if data.empty:
            print(f"→ Aucune donnée valide après nettoyage pour {ticker}.")
        
        print(f"--- Fin fetch_stock_data pour {ticker} (succès) ---")
        return data

    except KeyError as ke:
        msg = str(ke)
        # Essayer d'extraire le nom de la colonne du message d'erreur KeyError si possible
        # Un message KeyError typique pour une colonne manquante est "'nom_colonne'"
        col_name_match = pd.io.common.is_string_like(msg) and pd.io.common.get_match(msg, r"'([^']*)'")
        actual_col_missing = col_name_match.group(1) if col_name_match else msg

        print(f"Erreur Yahoo Finance (KeyError) pour {ticker}: La colonne {actual_col_missing} n'a pas été trouvée.")
        print(f"Colonnes disponibles au moment de l'erreur pour {ticker}: {data.columns.tolist() if 'data' in locals() and hasattr(data, 'columns') else 'data non défini ou sans colonnes'}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erreur Yahoo Finance (Autre) pour {ticker}: {type(e).__name__} - {str(e)}")
        if 'data' in locals() and hasattr(data, 'columns'):
             print(f"Colonnes disponibles au moment de l'erreur : {data.columns.tolist()}")
        return pd.DataFrame()



def save_to_mysql(df):
    if df.empty:
        print("Aucune donnée à insérer (DataFrame vide).")
        return

    print(f"DÉBOGAGE PyMySQL - Configuration MySQL utilisée (EN DUR) : {MYSQL_CONFIG}")
    try:
        with pymysql.connect(**MYSQL_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS historical_prices (
                        date DATE NOT NULL, ticker VARCHAR(10) NOT NULL, open DECIMAL(10,2) NOT NULL,
                        high DECIMAL(10,2) NOT NULL, low DECIMAL(10,2) NOT NULL, close DECIMAL(10,2) NOT NULL,
                        volume BIGINT NOT NULL, last_updated DATETIME NOT NULL, PRIMARY KEY (date, ticker)
                    )""")
                sql_query = """
                    REPLACE INTO historical_prices (date, ticker, open, high, low, close, volume, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                data_tuples = [tuple(x) for x in df.to_numpy()]
                if data_tuples:
                    cursor.executemany(sql_query, data_tuples)
                    conn.commit()
                    print(f"Données (premières 5 lignes) envoyées à MySQL avec PyMySQL:\n{df.head()}")
                    print(f"→ {len(df)} lignes insérées/actualisées dans MySQL avec PyMySQL.")
                else:
                    print("Aucune donnée à insérer après conversion en tuples.")
    except pymysql.Error as err:
        print(f"Erreur PyMySQL: {err}")
    except Exception as e:
        print(f"Erreur générale dans save_to_mysql: {e}")

if __name__ == "__main__":
    print("INFO: Exécution avec configuration MySQL en dur pour test (sans .env).")
    # LA VÉRIFICATION os.getenv A ÉTÉ SUPPRIMÉE ICI POUR CE TEST
    # # if not all(os.getenv(key) for key in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]):
    # #     print("ERREUR: Variables d'environnement MySQL manquantes. Vérifiez votre fichier .env et l'appel à load_dotenv().")
    # #     exit()

    start_date, end_date = get_rolling_year_dates()
    print(f"Période analysée : {start_date} au {end_date}")

    for ticker in TICKERS:
        print(f"\nTraitement de {ticker}...")
        df_ticker = fetch_stock_data(ticker, start_date, end_date)
        if not df_ticker.empty:
            save_to_mysql(df_ticker)

    print("\nPipeline terminé.") # Message légèrement ajusté
