import psycopg2
import traceback

def clean_and_alter_draft_combine_stats():
    try:
        print("Début du script de nettoyage pour draft_combine_stats")

        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            dbname="NBA",
            user="admin",
            password="admin"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Vérifier lignes avant nettoyage
        cur.execute("SELECT COUNT(*) FROM draft_combine_stats;")
        print("Nombre de lignes avant suppression des doublons :", cur.fetchone()[0])

        # Supprimer les doublons sur (player_id, season)
        cur.execute("""
        DELETE FROM draft_combine_stats dcs
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM draft_combine_stats
            GROUP BY player_id, season
        );
        """)
        print("Doublons supprimés.")

        # Colonnes à convertir en float (remplace None/mauvais formats)
        colonnes_float = [
            'height_wo_shoes', 'height_w_shoes', 'weight', 'wingspan', 'standing_reach',
            'body_fat_pct', 'hand_length', 'hand_width', 'standing_vertical_leap',
            'max_vertical_leap', 'lane_agility_time', 'modified_lane_agility_time',
            'three_quarter_sprint', 'bench_press'
        ]

        for col in colonnes_float:
            print(f"Conversion de {col} en FLOAT")
            cur.execute(f"""
                ALTER TABLE draft_combine_stats
                ALTER COLUMN {col} TYPE FLOAT USING NULLIF({col}::TEXT, '')::FLOAT;
            """)

        # Colonnes à convertir en INTEGER
        cur.execute("""
            ALTER TABLE draft_combine_stats
                ALTER COLUMN player_id TYPE INTEGER USING player_id::INTEGER,
                ALTER COLUMN season TYPE INTEGER USING season::INTEGER;
        """)
        print("Conversion des colonnes player_id et season en INTEGER")

        # Ajout de la clé primaire si non existante
        try:
            cur.execute("""
                ALTER TABLE draft_combine_stats
                ADD CONSTRAINT pk_draft_combine PRIMARY KEY (player_id, season);
            """)
            print("Clé primaire (player_id, season) ajoutée.")
        except Exception as e:
            print("Clé primaire déjà existante ou erreur :", e)

        cur.close()
        conn.close()

        print("Nettoyage et mise à jour de draft_combine_stats terminé avec succès.")

    except Exception:
        print("Erreur rencontrée :")
        traceback.print_exc()

# Appel du script
clean_and_alter_draft_combine_stats()
