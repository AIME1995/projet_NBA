import psycopg2
import traceback

def clean_and_alter_common_player_info():
    try:
        print("Début script")  # Ajouté pour vérifier l'entrée dans la fonction

        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            dbname="NBA",
            user="admin",
            password="admin"
        )
        conn.autocommit = True
        cur = conn.cursor()

        print("Nombre de lignes avant suppression doublons:")
        cur.execute("SELECT COUNT(*) FROM common_player_info;")
        print(cur.fetchone()[0])

        # Supprimer doublons
        cur.execute("""
        DELETE FROM common_player_info cpi
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM common_player_info
            GROUP BY person_id
        );
        """)
        print("Doublons supprimés.")

        print("Nombre de lignes après suppression doublons:")
        cur.execute("SELECT COUNT(*) FROM common_player_info;")
        print(cur.fetchone()[0])

        # Vérifier quelques birthdates avant update
        print("Exemples birthdate avant nettoyage:")
        cur.execute("SELECT birthdate FROM common_player_info LIMIT 5;")
        print(cur.fetchall())

        # Nettoyer birthdate
        cur.execute("""
    UPDATE common_player_info
    SET birthdate = 
        SUBSTRING(birthdate::TEXT FROM 1 FOR 4) || '-' ||
        SUBSTRING(birthdate::TEXT FROM 5 FOR 2) || '-' ||
        SUBSTRING(birthdate::TEXT FROM 7 FOR 2)
    WHERE birthdate IS NOT NULL AND birthdate::TEXT !~ '^\\d{4}-\\d{2}-\\d{2}$';
""")

        print("Birthdate nettoyé.")

        # Vérifier après
        print("Exemples birthdate après nettoyage:")
        cur.execute("SELECT birthdate FROM common_player_info LIMIT 5;")
        print(cur.fetchall())

        # Convertir en date
        cur.execute("""
        ALTER TABLE common_player_info
        ALTER COLUMN birthdate TYPE DATE USING birthdate::DATE;
        """)
        print("Type birthdate modifié en DATE.")

        # Autres colonnes
        cur.execute("""
        ALTER TABLE common_player_info
            ALTER COLUMN person_id TYPE INTEGER USING person_id::INTEGER,
            ALTER COLUMN height TYPE INTEGER USING height::INTEGER,
            ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
            ALTER COLUMN season_exp TYPE FLOAT USING season_exp::FLOAT,
            ALTER COLUMN jersey TYPE INTEGER USING jersey::INTEGER,
            ALTER COLUMN team_id TYPE INTEGER USING team_id::INTEGER,
            ALTER COLUMN from_year TYPE FLOAT USING from_year::FLOAT,
            ALTER COLUMN to_year TYPE FLOAT USING to_year::FLOAT;
        """)
        print("Colonnes modifiées avec nouveaux types.")

        # Tester les doublons
        cur.execute("""
        SELECT person_id, COUNT(*) FROM common_player_info GROUP BY person_id HAVING COUNT(*) > 1;
        """)
        duplicates = cur.fetchall()
        if duplicates:
            print(f"Doublons détectés sur person_id : {duplicates}")
        else:
            try:
                cur.execute("ALTER TABLE common_player_info ADD CONSTRAINT pk_common_player_info PRIMARY KEY (person_id);")
                print("Clé primaire ajoutée.")
            except Exception as e:
                print(f"Erreur lors de l'ajout de la clé primaire : {e}")

        cur.close()
        conn.close()

        print("Mise à jour terminée avec succès.")

    except Exception:
        print("Erreur rencontrée :")
        traceback.print_exc()

# Appel direct
clean_and_alter_common_player_info() 
