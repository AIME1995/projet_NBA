# -*- coding: utf-8 -*-

import psycopg2

def clean_and_alter_team_details():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            dbname="NBA",
            user="admin",
            password="admin",
            client_encoding="LATIN1"  # ← Ajouté pour éviter l'erreur d'encodage
        )
        conn.autocommit = True
        cur = conn.cursor()

        print("Suppression des doublons sur 'team_id'...")
        cur.execute("""
        DELETE FROM team_details
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM team_details
            GROUP BY team_id
        );
        """)

        print("Conversion de 'yearfounded' en entier...")
        cur.execute("""
        ALTER TABLE team_details
        ALTER COLUMN yearfounded TYPE INTEGER
        USING CAST(FLOOR(CAST(yearfounded AS NUMERIC)) AS INTEGER);
        """)

        print("Ajout de la clé primaire sur 'team_id'...")
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints 
                WHERE constraint_name = 'pk_team_details'
                  AND table_name = 'team_details'
            ) THEN
                ALTER TABLE team_details
                ADD CONSTRAINT pk_team_details PRIMARY KEY (team_id);
            END IF;
        END
        $$;
        """)

        print("✅ Traitement terminé pour la table 'team_details'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Script lancé...")
    clean_and_alter_team_details()
