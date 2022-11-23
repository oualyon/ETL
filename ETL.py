'''
ETL Projet Ydays 22-23 : OUALYON
'''

# Imports
import requests as rq
import pandas as pd

# Requete à l'API du gouvernement
response = rq.get("https://equipements.sports.gouv.fr/api/records/1.0/search/?dataset=data-es&q=&refine.code_dept=69&rows=6000")
data = response.json()

# Création du DataFrame
dataframe = pd.DataFrame(columns=['Nom', 'Commune', 'Type','Coord_X', 'Coord_Y'])

# Alimentation du DataFrame
for record in data['records']:
    rc = record['fields']
    dataframe.loc[len(dataframe.index)] = [rc['nominstallation'],rc['nom_commune'],rc['typequipement'],rc['coordgpsx'],rc['coordgpsy']]

# Export en csv
dataframe.to_csv('activities2.csv')