import psycopg2

def clean_and_alter_player():
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

        print("Suppression des doublons sur 'id'...")
        cur.execute("""
        DELETE FROM player p
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM player
            GROUP BY id
        );
        """)

        print("Ajout de la clé primaire sur 'id'...")
        cur.execute("""
        ALTER TABLE player
        ADD CONSTRAINT pk_player PRIMARY KEY (id);
        """)

        print("✅ Traitement terminé pour la table 'player'.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_player()
