import psycopg2

def clean_and_alter_officials():
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

        print("Suppression des doublons sur (game_id, official_id)...")
        cur.execute("""
        DELETE FROM officials o
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM officials
            GROUP BY game_id, official_id
        );
        """)

        print("Nettoyage des valeurs de jersey_num...")
        cur.execute("""
        UPDATE officials
        SET jersey_num = REGEXP_REPLACE(jersey_num::TEXT, '\\.0$', '')
        WHERE jersey_num::TEXT LIKE '%.0';
        """)

        print("Conversion de jersey_num en INTEGER...")
        cur.execute("""
        ALTER TABLE officials
        ALTER COLUMN jersey_num TYPE INTEGER
        USING jersey_num::INTEGER;
        """)

        print("Ajout de la clé primaire (game_id, official_id)...")
        cur.execute("""
        ALTER TABLE officials
        ADD CONSTRAINT pk_officials PRIMARY KEY (game_id, official_id);
        """)

        cur.close()
        conn.close()
        print("✅ Traitement terminé pour la table 'officials'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_officials()
