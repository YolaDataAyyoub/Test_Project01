import pandas as pd
from yola_functions import get_dataframe_metabase, toGoogleSheets, fromGoogleSheets
import sys

SAMPLE_SPREADSHEET_ID = '1O5NYU7p8-TG-IAj7MnyX0GRgfXu8JRW3lLmS7sj9bQc'
SAMPLE_SPREADSHEET_ID_HIST ='1RHvV-fct4KHwRTu4iArDXcIKid0T8bgzZ8KlOJaxz5U'
sheet_name = 'Sheet1'


# Créer un DataFrame à partir des données fournies
Df_pointage = fromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME=sheet_name + '!A1:AC')
print(Df_pointage)
Df_pointage['DT'] = pd.to_datetime(Df_pointage['Date de pointage'] + ' ' + Df_pointage['L\'heure'],
                                   format='%d/%m/%Y %H:%M:%S')

# Filtrer les entrées et les sorties du marché
marche_entries = Df_pointage[(Df_pointage['Type'] == 'Entrée') & (Df_pointage['Case'] == 'Marché')]
marche_sorties = Df_pointage[(Df_pointage['Type'] == 'Sortie') & (Df_pointage['Case'] == 'Marché')]

# Filtrer les entrées et les sorties du dépôt
depot_entries = Df_pointage[(Df_pointage['Type'] == 'Entrée') & (Df_pointage['Case'] == 'Dépôt')]
depot_sorties = Df_pointage[(Df_pointage['Type'] == 'Sortie') & (Df_pointage['Case'] == 'Dépôt')]

employes = Df_pointage["Nom d'employé"].unique()
dates_pointage = Df_pointage['Date de pointage'].unique()

result = pd.DataFrame(columns=['Nom d\'employé', 'Date de pointage', 'Heure entrée marché', 'Heure sortie marché',
                                'Durée au marché', 'Heure entrée dépôt', 'Heure sortie dépôt', 'Durée au dépôt', 'l\'heure de pointage'])

for employe in employes:
    for date in dates_pointage:
        # Filtrer les données pour l'employé et la date actuels
        marche_entree = marche_entries[(marche_entries["Nom d'employé"] == employe) &
                                       (marche_entries['Date de pointage'] == date)]
        marche_sortie = marche_sorties[(marche_sorties["Nom d'employé"] == employe) &
                                        (marche_sorties['Date de pointage'] == date)]
        depot_entree = depot_entries[(depot_entries["Nom d'employé"] == employe) &
                                      (depot_entries['Date de pointage'] == date)]
        depot_sortie = depot_sorties[(depot_sorties["Nom d'employé"] == employe) &
                                      (depot_sorties['Date de pointage'] == date)]

        # Initialiser les heures d'entrée et de sortie à NaN
        heure_entree_marche = heure_sortie_marche = pd.NaT
        heure_entree_depot = heure_sortie_depot = pd.NaT

        # Calculer l'heure d'entrée et de sortie pour le marché
        if not marche_entree.empty:
            heure_entree_marche = marche_entree.iloc[-1]['L\'heure']
        if not marche_sortie.empty:
            heure_sortie_marche = marche_sortie.iloc[-1]['L\'heure']

        # Calculer l'heure d'entrée et de sortie pour le dépôt
        if not depot_entree.empty:
            heure_entree_depot = depot_entree.iloc[-1]['L\'heure']
        if not depot_sortie.empty:
            heure_sortie_depot = depot_sortie.iloc[-1]['L\'heure']

        # Calculer la durée au marché
        if not marche_entree.empty and not marche_sortie.empty:
            duree_marche = marche_sortie.iloc[-1]['DT'] - marche_entree.iloc[-1]['DT']
            duree_marche_str = str(duree_marche)
            duree_marche = duree_marche_str.split(" ")[-1]  # Extraire la partie temps de la chaîne de caractères
        else:
            duree_marche = '00:00:00'

        # Calculer la durée au dépôt
        if not depot_entree.empty and not depot_sortie.empty:
            duree_depot = depot_sortie.iloc[-1]['DT'] - depot_entree.iloc[-1]['DT']
            duree_depot_str = str(duree_depot)
            duree_depot = duree_depot_str.split(" ")[-1]  # Extraire la partie temps de la chaîne de caractères
        else:
            duree_depot = '00:00:00'

        # Extract the last 'heure de pointage' for each employee on the same date of pointage
        last_heure_pointage = Df_pointage[(Df_pointage["Nom d'employé"] == employe) &
                                   (Df_pointage['Date de pointage'] == date)]['l\'heure de pointage'].iloc[-1]


        # Vérifier si l'heure de pointage existe
        heure_pointage = last_heure_pointage if pd.notna(last_heure_pointage) else pd.NaT

        # Ajouter une ligne au DataFrame résultant
        result = pd.concat([result, pd.DataFrame({'Nom d\'employé': [employe],
                                                   'Date de pointage': [date],
                                                   'Heure entrée marché': [heure_entree_marche],
                                                   'Heure sortie marché': [heure_sortie_marche],
                                                   'Durée au marché': [duree_marche],
                                                   'Heure entrée dépôt': [heure_entree_depot],
                                                   'Heure sortie dépôt': [heure_sortie_depot],
                                                   'Durée au dépôt': [duree_depot],
                                                   'l\'heure de pointage': [heure_pointage]})], ignore_index=True)

# Trier le DataFrame en fonction de la date de pointage
Historique_pointage = result.sort_values(by='Date de pointage')

# Supprimer le symbole "+" des durées au dépôt positives
Historique_pointage['Durée au dépôt'] = Historique_pointage['Durée au dépôt'].str.replace('+', '', regex=False)

Df_HIST = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_HIST, SHEET_NAME=sheet_name + '!A1:AC')
last_row = len(Df_HIST) + 2
rangeClear = f"A{last_row}:Z"
rangeUpdate= f"A{last_row}"

rangeClear2 = "A2:Z"
rangeUpdate2 = "A"
toGoogleSheets(Historique_pointage, SAMPLE_SPREADSHEET_ID_HIST, sheet_name, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
toGoogleSheets("", SAMPLE_SPREADSHEET_ID, sheet_name, rangeClear2, rangeUpdate2, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

print(Historique_pointage)
