import pandas as pd
import psycopg2

# Charger le DataFrame
df = pd.read_csv("data/team_history.csv")  # ajuste le chemin si besoin

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5434,
        dbname="NBA",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO team_history (team_id, city, nickname, year_founded, year_active_till)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            int(row['team_id']),
            row['city'],
            row['nickname'],
            int(row['year_founded']),
            int(row['year_active_till'])
        ))

    conn.commit()
    print("✅ Données insérées dans team_history avec succès.")

except Exception as e:
    print(f"❌ Erreur : {e}")

finally:
    if cur: cur.close()
    if conn: conn.close()
