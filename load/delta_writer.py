from deltalake import write_deltalake
from datetime import datetime
import os


def write_players_delta(df):
    now = datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")

    path = f"data_lake/raw/steam_players/year={year}/month={month}/day={day}/hour={hour}"

    os.makedirs(path, exist_ok = True)

    write_deltalake(path, df, mode = "append")

def write_metadata_delta(df):

    path = "data_lake/raw/steam_games_metadata"

    os.makedirs(path, exist_ok=True)

    write_deltalake(path, df, mode = "overwrite")