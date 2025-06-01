import pandas as pd
import psycopg2

def insert_team_data():
    # Charger les données
    df = pd.read_csv("data/team.csv")  # Mets le bon chemin si nécessaire

    # Nettoyer la colonne year_founded
    df['year_founded'] = df['year_founded'].fillna(0).astype(int)

    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            dbname="NBA",
            user="admin",
            password="admin"
        )
        cur = conn.cursor()

        print("Insertion des données dans la table 'team'...")

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO team (id, full_name, abbreviation, nickname, city, state, year_founded)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                int(row['id']),
                row['full_name'],
                row['abbreviation'],
                row['nickname'],
                row['city'],
                row['state'],
                int(row['year_founded'])
            ))

        conn.commit()
        print("✅ Données insérées avec succès dans la table 'team'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__ == "__main__":
    print("Script lancé...")
    insert_team_data()
