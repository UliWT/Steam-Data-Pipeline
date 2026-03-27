from deltalake import DeltaTable
import pandas as pd


def load_players_data():
    """Carga datos de jugadores desde Delta Lake (raw)."""

    base_path = "data_lake/raw/steam_players"

    try:
        dt = DeltaTable(base_path)
        return dt.to_pandas()
    except:
        print("⚠️ No se pudo leer la tabla de players")
        return pd.DataFrame()


def load_metadata_data():
    """Carga metadata de juegos desde Delta Lake."""
    dt = DeltaTable("data_lake/raw/steam_games_metadata")
    return dt.to_pandas()


def process_players_data(players_df, metadata_df):
    """
    Aplica transformaciones sobre los datos:
    limpieza, join y agregación.
    """

    # Evitar errores si no hay datos
    if players_df.empty:
        return pd.DataFrame()

    players_df = players_df.reset_index(drop=True)
    metadata_df = metadata_df.reset_index(drop=True)

    # Limpieza y renombrado
    players_df = players_df.dropna()
    players_df = players_df.rename(columns={"players": "player_count"})

    # Validación básica
    if "app_id" not in players_df.columns or "app_id" not in metadata_df.columns:
        raise Exception("Falta columna app_id")

    # Tomar último registro por juego (dato actual)
    players_df = players_df.sort_values(by="timestamp", ascending=False)
    players_df = players_df.groupby("game").head(1)

    # Evitar duplicados en el join
    if "game" in metadata_df.columns:
        metadata_df = metadata_df.drop(columns=["game"])

    # JOIN
    df = players_df.merge(metadata_df, on="app_id", how="left")

    # Agregación (promedio histórico)
    df["avg_players"] = df.groupby("game")["player_count"].transform("mean")

    # Limpieza final
    if "timestamp" in df.columns:
        df = df.drop(columns=["timestamp"])

    # Orden y formato final
    df = df.sort_values(by="player_count", ascending=False)
    df = df.reset_index(drop=True)
    df.index = df.index + 1

    return df