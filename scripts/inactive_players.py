import psycopg2

def clean_and_alter_inactive_players():
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

        print("Suppression des doublons sur player_id et game_id...")
        cur.execute("""
        DELETE FROM inactive_players ip
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM inactive_players
            GROUP BY player_id, game_id
        );
        """)

        print("Nettoyage et conversion de jersey_num en INTEGER...")
        cur.execute("""
            ALTER TABLE inactive_players
            ALTER COLUMN jersey_num TYPE INTEGER
            USING CAST(CAST(TRIM(jersey_num::TEXT) AS FLOAT) AS INTEGER);
        """)

        print("Conversion des colonnes entières en INTEGER si nécessaire...")
        colonnes_int = ['player_id', 'game_id', 'team_id']
        for col in colonnes_int:
            print(f"Conversion de {col} en INTEGER...")
            cur.execute(f"""
                ALTER TABLE inactive_players
                ALTER COLUMN {col} TYPE INTEGER
                USING TRIM({col}::TEXT)::INTEGER;
            """)

        print("Ajout d'une clé primaire sur (player_id, game_id)...")
        try:
            cur.execute("""
                ALTER TABLE inactive_players
                ADD CONSTRAINT pk_inactive_players PRIMARY KEY (player_id, game_id);
            """)
            print("Clé primaire ajoutée avec succès.")
        except psycopg2.errors.UniqueViolation:
            print("Erreur : des doublons existent encore.")
        except Exception as e:
            print(f"Erreur lors de l'ajout de la clé primaire : {e}")

        cur.close()
        conn.close()
        print("✅ Traitement terminé pour la table 'inactive_players'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_inactive_players()
