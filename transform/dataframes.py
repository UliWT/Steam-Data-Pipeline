from config.games import GAMES
from extract.steam_api import get_game_metadata, get_player_count

import pandas as pd
from datetime import datetime

def build_players_dataframe(games):

    playerCount_rows = []

    for app_id, name in GAMES.items():

        players = get_player_count(app_id)

        row = {
            "app_id": app_id,
            "game": name,
            "players": players,
            "timestamp": datetime.now()
        }
        if players is not None:
            playerCount_rows.append(row)

    df = pd.DataFrame(playerCount_rows)
    return df

def build_metadata_dataframe(games):

    metadata_rows = []

    for app_id, name in GAMES.items():

       metadata = get_game_metadata(app_id)

       if metadata is not None:

        row = {
            "app_id": metadata["app_id"],
            "game": metadata["name"],
            "developer": metadata["developer"],
            "publisher": metadata["publisher"]
        }
        
        metadata_rows.append(row)

    df = pd.DataFrame(metadata_rows)

    return df