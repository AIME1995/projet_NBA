import psycopg2

def clean_and_alter_line_score():
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

        print("Suppression des doublons sur (team_id_home, game_id)...")
        cur.execute("""
        DELETE FROM line_score ls
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM line_score
            GROUP BY team_id_home, game_id
        );
        """)

        print("CAST temporaire de game_date_est en TEXT pour le nettoyage...")
        cur.execute("""
        ALTER TABLE line_score
        ALTER COLUMN game_date_est TYPE TEXT;
        """)

        print("Nettoyage et formatage de game_date_est en 'YYYY-MM-DD'...")
        cur.execute("""
        UPDATE line_score
        SET game_date_est = 
            SUBSTRING(game_date_est FROM 1 FOR 4) || '-' ||
            SUBSTRING(game_date_est FROM 5 FOR 2) || '-' ||
            SUBSTRING(game_date_est FROM 7 FOR 2);
        """)

        print("Conversion de game_date_est en DATE...")
        cur.execute("""
        ALTER TABLE line_score
        ALTER COLUMN game_date_est TYPE DATE
        USING TO_DATE(game_date_est, 'YYYY-MM-DD');
        """)

        print("Ajout d'une clé primaire sur (team_id_home, game_id)...")
        try:
            cur.execute("""
                ALTER TABLE line_score
                ADD CONSTRAINT pk_line_score PRIMARY KEY (team_id_home, game_id);
            """)
            print("Clé primaire ajoutée avec succès.")
        except psycopg2.errors.UniqueViolation:
            print("Erreur : des doublons existent encore.")
        except Exception as e:
            print(f"Erreur lors de l'ajout de la clé primaire : {e}")

        cur.close()
        conn.close()
        print("✅ Traitement terminé pour la table 'line_score'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_line_score()
