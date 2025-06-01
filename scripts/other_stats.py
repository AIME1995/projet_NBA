import psycopg2

def clean_and_alter_other_stats():
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

        print("Suppression des doublons sur (game_id, team_id_home)...")
        cur.execute("""
        DELETE FROM other_stats o
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM other_stats
            GROUP BY game_id, team_id_home
        );
        """)

        print("Conversion des colonnes float en INTEGER...")
        colonnes_float = [
            'team_turnovers_home', 'total_turnovers_home', 'team_rebounds_home', 'pts_off_to_home',
            'team_turnovers_away', 'total_turnovers_away', 'team_rebounds_away', 'pts_off_to_away'
        ]

        for col in colonnes_float:
            print(f"  ➤ Conversion de {col}...")
            cur.execute(f"""
                ALTER TABLE other_stats
                ALTER COLUMN {col} TYPE INTEGER
                USING ROUND({col}::numeric)::INTEGER;
            """)

        print("Ajout de la clé primaire (game_id, team_id_home)...")
        cur.execute("""
        ALTER TABLE other_stats
        ADD CONSTRAINT pk_other_stats PRIMARY KEY (game_id, team_id_home);
        """)

        cur.close()
        conn.close()
        print("✅ Traitement terminé pour la table 'other_stats'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_other_stats()
