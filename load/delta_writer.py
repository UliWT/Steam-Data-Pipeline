from deltalake import write_deltalake
from datetime import datetime
import os


def write_players_raw(df):
    """Guarda datos de jugadores en Delta Lake (raw/bronze) con particionado temporal."""

    now = datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")

    # Columnas de partición
    df["year"] = year
    df["month"] = month
    df["day"] = day
    df["hour"] = hour

    path = "data_lake/raw/steam_players"
    os.makedirs(path, exist_ok=True)

    write_deltalake(
        path,
        df,
        mode="append",  # ingestión incremental
        partition_by=["year", "month", "day", "hour"]
    )


def write_metadata_raw(df):
    """Guarda metadata de juegos en (raw/bronze)."""

    path = "data_lake/raw/steam_games_metadata"
    os.makedirs(path, exist_ok=True)

    write_deltalake(
        path,
        df,
        mode="overwrite"
    )


def write_silver_players(df):
    """Guarda datos de jugadores limpios en la capa Silver."""
    path = "data_lake/silver/steam_players"
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, df, mode="overwrite")


def write_silver_metadata(df):
    """Guarda metadata de juegos en la capa Silver."""
    path = "data_lake/silver/steam_games_metadata"
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, df, mode="overwrite")


import shutil

def write_gold_players_stats(df):
    """Guarda datos agregados y finales en la capa Gold."""
    path = "data_lake/gold/steam_players_stats"
    
    # Limpieza manual para evitar errores de esquema
    if os.path.exists(path):
        shutil.rmtree(path)
    
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, df, mode="overwrite")