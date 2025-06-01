import psycopg2
import traceback
from tqdm import tqdm

def clean_and_alter_play_by_play():
    try:
        print("\n🚀 Début du script de nettoyage pour play_by_play\n")

        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            dbname="NBA",
            user="admin",
            password="admin"
        )
        conn.autocommit = True
        cur = conn.cursor()

        steps = [
            "Vérification du nombre de lignes",
            "Suppression des doublons",
            "Conversion des colonnes en INTEGER",
            "Conversion des colonnes en TEXT",
            "Ajout de la clé primaire"
        ]

        for step in tqdm(steps, desc="🛠️ Progression du nettoyage", unit="étape"):
            if step == "Vérification du nombre de lignes":
                cur.execute("SELECT COUNT(*) FROM play_by_play;")
                count = cur.fetchone()[0]
                print(f"\n➡️ Nombre de lignes avant suppression des doublons : {count}")

            elif step == "Suppression des doublons":
                cur.execute("""
                    DELETE FROM play_by_play pbp
                    WHERE ctid NOT IN (
                        SELECT MIN(ctid)
                        FROM play_by_play
                        GROUP BY game_id, eventnum
                    );
                """)
                print("✅ Doublons supprimés.")

            elif step == "Conversion des colonnes en INTEGER":
                colonnes_int = ['game_id', 'eventnum', 'eventmsgtype', 'eventmsgactiontype', 'period']
                for col in colonnes_int:
                    print(f"🔄 Conversion de {col} en INTEGER...")
                    cur.execute(f"""
                        ALTER TABLE play_by_play
                        ALTER COLUMN {col} TYPE INTEGER USING NULLIF({col}::TEXT, '')::INTEGER;
                    """)

            elif step == "Conversion des colonnes en TEXT":
                colonnes_text = ['wctimestring', 'pctimestring', 'homedescription', 'neutraldescription', 'visitordescription']
                for col in colonnes_text:
                    print(f"🔤 Conversion de {col} en TEXT...")
                    cur.execute(f"""
                        ALTER TABLE play_by_play
                        ALTER COLUMN {col} TYPE TEXT;
                    """)

            elif step == "Ajout de la clé primaire":
                try:
                    cur.execute("""
                        ALTER TABLE play_by_play
                        ADD CONSTRAINT pk_play_by_play PRIMARY KEY (game_id, eventnum);
                    """)
                    print("🧩 Clé primaire ajoutée.")
                except Exception as e:
                    print(f"⚠️ Clé primaire déjà existante ou erreur : {e}")

        cur.close()
        conn.close()

        print("\n✅ Nettoyage et transformation de play_by_play terminé avec succès.\n")

    except Exception:
        print("\n❌ Erreur rencontrée pendant le nettoyage :")
        traceback.print_exc()

# Appel du script
clean_and_alter_play_by_play()
