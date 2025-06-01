import psycopg2
from psycopg2 import sql, errors

def clean_and_alter_draft_history_table():
    print("Début du traitement de la table 'draft_history'...")

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

        # Vérification du nombre de lignes avant
        cur.execute("SELECT COUNT(*) FROM draft_history;")
        total_before = cur.fetchone()[0]
        print(f"Nombre total de lignes avant suppression des doublons : {total_before}")

        # Suppression des doublons (garde la première occurrence par ctid)
        print("Suppression des doublons sur 'person_id'...")
        cur.execute("""
            DELETE FROM draft_history d
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM draft_history
                GROUP BY person_id
            );
        """)
        print("Doublons supprimés.")

        # Vérification du nombre de lignes après
        cur.execute("SELECT COUNT(*) FROM draft_history;")
        total_after = cur.fetchone()[0]
        print(f"Nombre total de lignes après suppression des doublons : {total_after}")

        # Conversion des colonnes en INTEGER
        columns_to_convert = [
            'person_id', 'season', 'round_number', 'round_pick',
            'overall_pick', 'team_id', 'player_profile_flag'
        ]
        print("Conversion des types des colonnes en INTEGER...")
        for col in columns_to_convert:
            print(f" - Conversion de {col} en INTEGER...")
            cur.execute(sql.SQL("""
                ALTER TABLE draft_history
                ALTER COLUMN {column} TYPE INTEGER USING {column}::INTEGER;
            """).format(column=sql.Identifier(col)))

        # Test doublons avant ajout de clé primaire
        print("Vérification de l'unicité de 'person_id' avant ajout de la clé primaire...")
        cur.execute("""
            SELECT person_id, COUNT(*)
            FROM draft_history
            GROUP BY person_id
            HAVING COUNT(*) > 1;
        """)
        duplicates = cur.fetchall()

        if duplicates:
            print("⚠️ Doublons détectés, impossible d'ajouter la clé primaire.")
            for d in duplicates:
                print(f" - person_id = {d[0]}, doublons = {d[1]}")
        else:
            try:
                cur.execute("""
                    ALTER TABLE draft_history
                    ADD CONSTRAINT pk_draft_history PRIMARY KEY (person_id);
                """)
                print("✅ Clé primaire ajoutée avec succès.")
            except Exception as e:
                print(f"❌ Erreur lors de l'ajout de la clé primaire : {e}")

        cur.close()
        conn.close()
        print("✅ Traitement terminé pour la table 'draft_history'.")

    except Exception as e:
        print(f"❌ Erreur globale : {e}")

if __name__ == "__main__":
    clean_and_alter_draft_history_table()
