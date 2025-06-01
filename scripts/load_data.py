# -*- coding: utf-8 -*-
import os
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from tqdm import tqdm

# Dossier contenant les fichiers CSV
csv_directory = os.path.join(os.path.dirname(__file__), '../data')

# Détails de la connexion à PostgreSQL
db_params = {
    'dbname': 'NBA',
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5434'
}

# Connexion à PostgreSQL
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Fonction pour créer une table
def create_table(df, table_name):
    create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name} (").format(
        table_name=sql.Identifier(table_name)
    )
    columns = [sql.Identifier(col) for col in df.columns]
    column_definitions = [sql.SQL("{} TEXT").format(col) for col in columns]
    create_table_query += sql.SQL(', ').join(column_definitions) + sql.SQL(");")
    cursor.execute(create_table_query)
    print(f"✅ Table {table_name} créée avec succès.")

# Fonction pour insérer les données
def insert_data(df, table_name, truncate_before_insert=False):
    if truncate_before_insert:
        cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name)))
        print(f"🧹 Table {table_name} vidée avant insertion.")
    
    insert_query = sql.SQL("INSERT INTO {table_name} ({fields}) VALUES ({placeholders})").format(
        table_name=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(map(sql.Identifier, df.columns)),
        placeholders=sql.SQL(', ').join([sql.Placeholder()] * len(df.columns))
    )

    data_tuples = [tuple(None if pd.isna(val) else val for val in row) for row in df.to_numpy()]
    psycopg2.extras.execute_batch(cursor, insert_query, data_tuples)
    print(f"📥 Données insérées dans {table_name}.")

# Liste des fichiers CSV à traiter
all_files = [f for f in os.listdir(csv_directory) if f.endswith(".csv")]
# Exclure play_by_play.csv au départ
other_files = [f for f in all_files if f != 'play_by_play.csv']
# Mettre play_by_play.csv à la fin
sorted_files = other_files + ['play_by_play.csv'] if 'play_by_play.csv' in all_files else other_files

# Parcourir les fichiers dans l'ordre défini
for filename in sorted_files:
    file_path = os.path.join(csv_directory, filename)
    table_name = os.path.splitext(filename)[0]

    try:
        print(f"\n📂 Traitement de {filename}...")

        if filename == 'play_by_play.csv':
            chunk_size = 100_000
            total_rows = sum(1 for _ in open(file_path)) - 1  # Exclure l'en-tête
            total_chunks = (total_rows // chunk_size) + 1

            chunk_iter = pd.read_csv(file_path, chunksize=chunk_size)
            first_chunk = True

            for i, chunk in enumerate(tqdm(chunk_iter, total=total_chunks, desc="📊 Insertion play_by_play")):
                if first_chunk:
                    create_table(chunk, table_name)
                    first_chunk = False
                insert_data(chunk, table_name)
                conn.commit()
        else:
            df = pd.read_csv(file_path)
            create_table(df, table_name)
            insert_data(df, table_name)
            conn.commit()
            print(f"✅ {filename} importé avec succès.\n")

    except Exception as e:
        print(f"❌ Erreur lors du traitement de {filename} : {e}\n")
        conn.rollback()

# Fermer la connexion
cursor.close()
conn.close()

print("🎯 Chargement de tous les fichiers CSV terminé.")
  