from config.games import GAMES

from transform.dataframes import (
    build_players_dataframe,
    build_metadata_dataframe
)

from load.delta_writer import (
    write_players_delta,
    write_metadata_delta,
    write_processed_players
)

from transform.processing import (
    load_players_data,
    load_metadata_data,
    process_players_data
)


def main():

    # ===============================
    # ETAPA 1: RAW
    # ===============================

    players_df = build_players_dataframe(GAMES)
    players_df = players_df.sort_values(by="players", ascending=False)
    players_df.index = players_df.index + 1

    write_players_delta(players_df)

    metadata_df = build_metadata_dataframe(GAMES)
    metadata_df.index = metadata_df.index + 1

    write_metadata_delta(metadata_df)

    # ===============================
    # ETAPA 2: PROCESSED
    # ===============================

    players_raw = load_players_data()
    metadata_raw = load_metadata_data()

    processed_df = process_players_data(players_raw, metadata_raw)

    print("\nProcessed data preview:")
    print(processed_df)

    if not processed_df.empty:
        write_processed_players(processed_df)


if __name__ == "__main__":
    main()