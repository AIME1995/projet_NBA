import psycopg2

def clean_and_alter_game_table():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            dbname="NBA",
            user="admin",
            password="admin"
        )
        conn.autocommit = True
        cur = conn.cursor()

        print("Suppression des doublons sur game_id...")
        cur.execute("""
        DELETE FROM game g
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM game
            GROUP BY game_id
        );
        """)

        print("Conversion de game_date en format DATE...")
        cur.execute("""
            UPDATE game
            SET game_date = TO_DATE(SUBSTRING(game_date::TEXT FROM 1 FOR 8), 'YYYYMMDD')
            WHERE game_date IS NOT NULL;
        """)

        print("Modification des types des colonnes...")

        # season_id conservé en texte, pas dans columns_to_int
        columns_to_int = [
            'team_id_home', 'game_id', 'min',
            'plus_minus_home', 'video_available_home', 'team_id_away',
            'plus_minus_away', 'video_available_away'
        ]

        for col in columns_to_int:
            print(f"ALTER COLUMN {col} en INTEGER...")
            cur.execute(f"ALTER TABLE game ALTER COLUMN {col} TYPE INTEGER USING {col}::INTEGER;")

        print("ALTER COLUMN game_date en DATE...")
        cur.execute("ALTER TABLE game ALTER COLUMN game_date TYPE DATE USING game_date::DATE;")

        columns_to_float = [
            'fgm_home', 'fga_home', 'fg_pct_home', 'fg3m_home', 'fg3a_home',
            'fg3_pct_home', 'ftm_home', 'fta_home', 'ft_pct_home', 'oreb_home',
            'dreb_home', 'reb_home', 'ast_home', 'stl_home', 'blk_home', 'tov_home',
            'pf_home', 'pts_home', 'fgm_away', 'fga_away', 'fg_pct_away', 'fg3m_away',
            'fg3a_away', 'fg3_pct_away', 'ftm_away', 'fta_away', 'ft_pct_away',
            'oreb_away', 'dreb_away', 'reb_away', 'ast_away', 'stl_away', 'blk_away',
            'tov_away', 'pf_away', 'pts_away'
        ]

        for col in columns_to_float:
            print(f"ALTER COLUMN {col} en FLOAT...")
            cur.execute(f"ALTER TABLE game ALTER COLUMN {col} TYPE FLOAT USING {col}::FLOAT;")

        print("Ajout de la clé primaire sur game_id...")
        try:
            cur.execute("ALTER TABLE game ADD CONSTRAINT pk_game PRIMARY KEY (game_id);")
            print("Clé primaire ajoutée.")
        except psycopg2.errors.UniqueViolation:
            print("Erreur : doublons restants sur game_id, impossible d'ajouter la clé primaire.")
        except Exception as e:
            print(f"Erreur inconnue lors de l'ajout de la clé primaire : {e}")

        cur.close()
        conn.close()
        print("Traitement terminé pour la table 'game'.")

    except Exception as e:
        print(f"Erreur : {e}")

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_game_table()
