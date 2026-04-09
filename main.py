from config.games import GAMES

from transform.dataframes import (
    build_players_dataframe,
    build_metadata_dataframe
)

from load.delta_writer import (
    write_players_raw,
    write_metadata_raw,
    write_silver_players,
    write_silver_metadata,
    write_gold_players_stats
)

from transform.processing import (
    load_raw_players,
    load_raw_metadata,
    load_silver_players,
    load_silver_metadata,
    clean_players_data,
    clean_metadata_data,
    create_gold_aggregations
)


def main():

    # ===============================
    # ETAPA 1: BRONZE (RAW)
    # ===============================
    print("\n--- ETAPA 1: BRONZE (RAW) ---")
    
    # Extraer y guardar jugadores
    players_raw_df = build_players_dataframe(GAMES)
    write_players_raw(players_raw_df)
    print("✅ Jugadores guardados en RAW")

    # Extraer y guardar metadata
    metadata_raw_df = build_metadata_dataframe(GAMES)
    write_metadata_raw(metadata_raw_df)
    print("✅ Metadata guardada en RAW")


    # ===============================
    # ETAPA 2: SILVER (CLEANED)
    # ===============================
    print("\n--- ETAPA 2: SILVER (CLEANED) ---")

    # Procesar players
    players_raw = load_raw_players()
    players_silver = clean_players_data(players_raw)
    write_silver_players(players_silver)
    print("✅ Jugadores procesados y guardados en SILVER")

    # Procesar metadata (según observación del profe)
    metadata_raw = load_raw_metadata()
    metadata_silver = clean_metadata_data(metadata_raw)
    write_silver_metadata(metadata_silver)
    print("✅ Metadata procesada y guardada en SILVER")


    # ===============================
    # ETAPA 3: GOLD (AGGREGATED)
    # ===============================
    print("\n--- ETAPA 3: GOLD (AGGREGATED) ---")

    # Cargar datos desde SILVER para las agregaciones
    p_silver = load_silver_players()
    m_silver = load_silver_metadata()

    # Crear tabla de agregaciones
    gold_df = create_gold_aggregations(p_silver, m_silver)

    print("\nGold data preview (Aggregations):")
    print(gold_df)

    if not gold_df.empty:
        write_gold_players_stats(gold_df)
        print("✅ Estadísticas finales guardadas en GOLD")


if __name__ == "__main__":
    main()
1