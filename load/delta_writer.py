from deltalake import write_deltalake
from datetime import datetime
import os


def write_players_delta(df):
    """Guarda datos de jugadores en Delta Lake (raw) con particionado temporal."""

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
        df.reset_index(drop=True),
        mode="append",  # ingestión incremental
        partition_by=["year", "month", "day", "hour"]
    )


def write_metadata_delta(df):
    """Guarda metadata de juegos (overwrite por ser datos estáticos)."""

    path = "data_lake/raw/steam_games_metadata"
    os.makedirs(path, exist_ok=True)

    write_deltalake(
        path,
        df.reset_index(drop=True),
        mode="overwrite"
    )


def write_processed_players(df):
    """Guarda datos procesados en la capa processed."""

    path = "data_lake/processed/steam_players"
    os.makedirs(path, exist_ok=True)

    write_deltalake(
        path,
        df.reset_index(drop=True),
        mode="overwrite",
    )