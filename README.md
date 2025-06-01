# Projet NBA - Analyse de Données Massives

## Description du projet

Ce projet consiste à intégrer de grandes quantités de données NBA, stockées dans des fichiers CSV, dans une base de données PostgreSQL déployée dans un conteneur Docker. Chaque fichier CSV correspond à une table spécifique de la base. Pour cela, nous avons créé un conteneur Docker PostgreSQL, dans lequel nous avons importé les données en définissant soigneusement les formats de colonnes, les clés primaires et les contraintes nécessaires. L’intégration a été automatisée via des scripts Python dédiés, un par fichier/table, afin d’assurer un chargement propre et reproductible.

Une fois les données intégrées et mises à jour dans la base, nous avons réalisé une série d’analyses pour extraire des informations pertinentes sur les joueurs, les équipes, leurs performances, ainsi que les tendances historiques des victoires.

---

## Structure du projet

C:.
│ docker-compose.yml # Déploiement du conteneur PostgreSQL
│ README.md # Documentation du projet
│
├───analyse_data
│ Analyse.ipynb # Notebook d’analyse des données intégrées
│
├───data # Fichiers CSV sources
│ common_player_info.csv
│ draft_combine_stats.csv
│ draft_history.csv
│ game.csv
│ game_info.csv
│ game_summary.csv
│ inactive_players.csv
│ line_score.csv
│ officials.csv
│ other_stats.csv
│ player.csv
│ play_by_play.csv
│ team.csv
│ team_details.csv
│ team_history.csv
│ team_info_common.csv
│
├───scripts # Scripts Python d’intégration par table
│ common-player_info.py
│ draft_combine_stats.py
│ draft_history.py
│ drop_table.csv
│ game.py
│ game_info.py
│ game_summary.py
│ inactive_players.py
│ line_score.py
│ load_data.py
│ load_draft_combine_play.csv
│ officials.py
│ other_stats.py
│ player.py
│ play_by_play.py
│ team.py
│ team_details.py
│ team_history.py
│ init.py
│
└───sql # Scripts SQL de création et insertion des tables
create_tables.sql
insert_data.sql




---

## Déroulement du projet

1. **Création et lancement du conteneur Docker PostgreSQL** à l’aide du fichier `docker-compose.yml`.

2. **Définition de la structure des tables** dans `create_tables.sql` (types de colonnes, clés primaires, contraintes).

3. **Chargement initial des données** avec `insert_data.sql` et les scripts Python présents dans le dossier `scripts/`, un script par table, permettant d’importer proprement chaque fichier CSV dans la base.

4. **Mise à jour des tables** pour ajuster les formats, nettoyer les données et garantir l’intégrité via les scripts Python dans le dossier `scripts/`.

5. **Analyse des données** via le notebook `analyse_data/Analyse.ipynb` pour répondre aux questions métier.

---

## Analyses réalisées

- Extraction des informations des joueurs et de leurs statistiques combinées.
- Identification des équipes gagnantes pour chaque match de la saison 2015.
- Calcul du nombre de victoires par équipe pour l’année 2018.
- Classement des joueurs ayant remporté le plus de victoires par équipe entre 1980 et 2000.
- Analyse des actions les plus fréquentes par équipes pour les années 1980 et 2015.

### BONUS

- Étude des tendances des performances des équipes gagnantes au fil du temps.



## Technologies utilisées

- **PostgreSQL** dans un conteneur **Docker** pour la gestion de la base de données.
- **Python** pour l’automatisation de l’import et du nettoyage des données (pandas, psycopg2).
- **SQL** pour la définition et manipulation des données.
- **Jupyter Notebook** pour les analyses exploratoires et visuelles.



## Instructions pour reproduire

1. Cloner le dépôt.
2. Lancer le conteneur Docker avec `docker-compose up -d`.
3. Exécuter les scripts SQL pour créer les tables.
4. Lancer les scripts Python dans le dossier `scripts/` pour importer les fichiers CSV.
5. Ouvrir le notebook `analyse_data/Analyse.ipynb` pour réaliser les analyses.



Ce projet montre comment intégrer efficacement des données massives dans une base relationnelle et exploiter ces données pour des analyses statistiques et exploratoires dans un contexte sportif.  
