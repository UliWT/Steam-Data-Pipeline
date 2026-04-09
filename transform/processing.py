from deltalake import DeltaTable
import pandas as pd

def _load_delta(path):
    """Función interna para evitar repetición de código de carga."""
    try:
        return DeltaTable(path).to_pandas()
    except:
        return pd.DataFrame()

# Funciones de carga 
def load_raw_players():    return _load_delta("data_lake/raw/steam_players")
def load_raw_metadata():   return _load_delta("data_lake/raw/steam_games_metadata")
def load_silver_players(): return _load_delta("data_lake/silver/steam_players")
def load_silver_metadata(): return _load_delta("data_lake/silver/steam_games_metadata")


def clean_players_data(df):
    """Limpia y normaliza los datos de jugadores para Silver."""
    if df.empty: return df
    
    return (df.dropna()
            .rename(columns={"players": "player_count"})
            .reset_index(drop=True))


def clean_metadata_data(df):
    """Limpia la metadata para Silver."""
    return df.reset_index(drop=True) if not df.empty else df


def create_gold_aggregations(p_silver, m_silver):
    """Crea el resumen final Gold (Último estado + Promedio)."""
    if p_silver.empty or m_silver.empty: return pd.DataFrame()

    # 1. Agregaciones: Promedio histórico
    avg = p_silver.groupby("app_id")["player_count"].mean().reset_index(name="avg_players")

    # 2. Último Snapshot (Deduplicación)
    latest = p_silver.sort_values("timestamp", ascending=False).groupby("app_id").head(1)

    # 3. Join y Enriquecimiento
    gold = latest.merge(avg, on="app_id")
    
    # Evitar duplicados de columnas (como 'game') al unir con metadata
    meta_cols = m_silver.columns.difference(gold.columns).tolist() + ["app_id"]
    gold = gold.merge(m_silver[meta_cols], on="app_id")

    # 4. Limpieza final y orden
    gold = (gold.drop(columns=["timestamp"], errors="ignore")
            .sort_values("player_count", ascending=False)
            .reset_index(drop=True))
    
    gold.index += 1 # Índice amigable (empieza en 1)
    
    return gold
