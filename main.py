from config.games import GAMES

from transform.dataframes import(
    build_players_dataframe,
    build_metadata_dataframe
)

from load.delta_writer import(
    write_players_delta,
    write_metadata_delta
)

def main():
    
    players_df = build_players_dataframe(GAMES)
    print("\n")
    players_df = players_df.sort_values(by="players", ascending= False)
    players_df.index = players_df.index + 1
    print(players_df)
    write_players_delta(players_df)


    metadata_df = build_metadata_dataframe(GAMES)
    print("\n")
    metadata_df.index = metadata_df.index + 1
    print(metadata_df)
    write_metadata_delta(metadata_df)

if __name__ == "__main__":
    main()