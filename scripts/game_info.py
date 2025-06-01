import psycopg2
from psycopg2 import sql, errors

def clean_and_alter_game_info_table():
    print("🔧 Début du traitement de la table 'game_info'...")

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

        # Suppression des doublons
        print("🗑️ Suppression des doublons sur 'game_id'...")
        cur.execute("""
            DELETE FROM game_info g
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM game_info
                GROUP BY game_id
            );
        """)

        # Nettoyage de la colonne game_date
        print("📅 Formatage de 'game_date' en 'YYYY-MM-DD'...")
        cur.execute("""
            UPDATE game_info
            SET game_date = TO_DATE(
                CASE
                    WHEN LENGTH(TRIM(game_date::TEXT)) = 8 AND game_date ~ '^[0-9]{8}$' THEN SUBSTRING(game_date::TEXT, 1, 8)
                    WHEN LENGTH(TRIM(game_date::TEXT)) = 6 AND game_date ~ '^[0-9]{6}$' THEN SUBSTRING(game_date::TEXT, 1, 6) || '01'
                    ELSE NULL
                END,
                'YYYYMMDD'
            )
            WHERE game_date IS NOT NULL AND TRIM(game_date::TEXT) <> '';
        """)

        # Colonnes à traiter
        temp_text_cols = ['game_id', 'game_time', 'attendance']
        int_cols = ['game_id', 'game_time']
        float_cols = ['attendance']

        print("🔄 Cast temporaire en TEXT pour nettoyage...")
        for col in temp_text_cols:
            print(f" - ALTER {col} → TEXT")
            cur.execute(sql.SQL("""
                ALTER TABLE game_info
                ALTER COLUMN {col} TYPE TEXT;
            """).format(col=sql.Identifier(col)))

        # Nettoyage des colonnes
        print("🧹 Nettoyage des colonnes...")
        for col in int_cols:
            print(f" - Suppression des '.0' dans {col}")
            cur.execute(sql.SQL("""
                UPDATE game_info
                SET {col} = REGEXP_REPLACE(TRIM({col}), '\\.0$', '')
                WHERE {col} ~ '\\.0$';
            """).format(col=sql.Identifier(col)))

            print(f" - Remplacement des vides dans {col} par NULL")
            cur.execute(sql.SQL("""
                UPDATE game_info
                SET {col} = NULL
                WHERE TRIM({col}) = '';
            """).format(col=sql.Identifier(col)))

        print(" - Nettoyage de 'attendance'")
        cur.execute("""
            UPDATE game_info
            SET attendance = NULL
            WHERE TRIM(attendance) = '';
        """)

        # Conversion des colonnes
        print("🔁 Conversion des colonnes...")
        for col in int_cols:
            print(f" - {col} → INTEGER")
            cur.execute(sql.SQL("""
                ALTER TABLE game_info
                ALTER COLUMN {col} TYPE INTEGER
                USING TRIM({col})::INTEGER;
            """).format(col=sql.Identifier(col)))

        print(" - attendance → FLOAT")
        cur.execute("""
            ALTER TABLE game_info
            ALTER COLUMN attendance TYPE FLOAT
            USING TRIM(attendance)::FLOAT;
        """)

        print(" - game_date → DATE")
        cur.execute("""
            ALTER TABLE game_info
            ALTER COLUMN game_date TYPE DATE
            USING game_date::DATE;
        """)

        # Vérification des doublons
        print("🔍 Vérification des doublons avant ajout de la clé primaire sur 'game_id'...")
        cur.execute("""
            SELECT game_id, COUNT(*)
            FROM game_info
            GROUP BY game_id
            HAVING COUNT(*) > 1;
        """)
        duplicates = cur.fetchall()

        if duplicates:
            print("⚠️ Doublons encore présents, clé primaire non ajoutée.")
            for dup in duplicates:
                print(f" - game_id = {dup[0]}, doublons = {dup[1]}")
        else:
            try:
                print("🔐 Ajout de la clé primaire sur 'game_id'...")
                cur.execute("""
                    ALTER TABLE game_info
                    ADD CONSTRAINT pk_game_info PRIMARY KEY (game_id);
                """)
                print("✅ Clé primaire ajoutée avec succès.")
            except Exception as e:
                print(f"❌ Erreur lors de l'ajout de la clé primaire : {e}")

        cur.close()
        conn.close()
        print("✅ Traitement terminé pour la table 'game_info'.")

    except Exception as e:
        print(f"❌ Erreur globale : {e}")

if __name__ == "__main__":
    clean_and_alter_game_info_table()
