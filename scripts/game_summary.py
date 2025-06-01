import psycopg2

def clean_and_alter_game_summary_table():
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
        DELETE FROM game_summary g
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM game_summary
            GROUP BY game_id
        );
        """)

        print("CAST temporaire de game_date_est en TEXT pour le nettoyage...")
        cur.execute("""
            ALTER TABLE game_summary
            ALTER COLUMN game_date_est TYPE TEXT;
        """)

        print("Nettoyage et formatage de game_date_est en 'YYYY-MM-DD'...")
        cur.execute("""
            UPDATE game_summary
            SET game_date_est = 
                SUBSTRING(game_date_est, 1, 4) || '-' || 
                SUBSTRING(game_date_est, 5, 2) || '-' || 
                SUBSTRING(game_date_est, 7, 2)
            WHERE game_date_est IS NOT NULL AND LENGTH(game_date_est) >= 8;
        """)

        print("Conversion de game_date_est en DATE...")
        cur.execute("""
            ALTER TABLE game_summary
            ALTER COLUMN game_date_est TYPE DATE
            USING game_date_est::DATE;
        """)

        print("Conversion des colonnes en types appropriés...")

        colonnes_int = ['game_id', 'game_status_id', 'home_team_id',
                        'visitor_team_id', 'season', 'live_period', 'wh_status']

        colonnes_float_to_int = ['game_sequence']

        for col in colonnes_int:
            print(f"Conversion de {col} en INTEGER...")
            cur.execute(f"""
                ALTER TABLE game_summary
                ALTER COLUMN {col} TYPE INTEGER
                USING TRIM({col}::TEXT)::INTEGER;
            """)

        for col in colonnes_float_to_int:
            print(f"Conversion de {col} en INTEGER depuis FLOAT...")
            cur.execute(f"""
                ALTER TABLE game_summary
                ALTER COLUMN {col} TYPE INTEGER
                USING CAST(CAST(TRIM({col}::TEXT) AS FLOAT) AS INTEGER);
            """)

        print("Conversion de live_pc_time en FLOAT...")
        cur.execute("""
            ALTER TABLE game_summary
            ALTER COLUMN live_pc_time TYPE FLOAT
            USING TRIM(live_pc_time::TEXT)::FLOAT;
        """)

        print("Ajout de la clé primaire sur game_id...")
        try:
            cur.execute("ALTER TABLE game_summary ADD CONSTRAINT pk_game_summary PRIMARY KEY (game_id);")
            print("Clé primaire ajoutée.")
        except psycopg2.errors.UniqueViolation:
            print("Erreur : doublons restants sur game_id, impossible d'ajouter la clé primaire.")
        except Exception as e:
            print(f"Erreur inconnue lors de l'ajout de la clé primaire : {e}")

        cur.close()
        conn.close()
        print("✅ Traitement terminé pour la table 'game_summary'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_game_summary_table()
