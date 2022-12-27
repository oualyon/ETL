'''
ETL Projet Ydays 22-23 : OUALYON
TODO Mettre à jour avec le code du workbook
'''

# Imports
import pandas as pd
import requests as rq
import json
import CategoryMap

# Création du DataFrame
dataframe = pd.DataFrame(columns=['Nom', 'Commune', 'Type','Coord_X', 'Coord_Y'])

# Requete à l'API du gouvernement
response = rq.get("https://equipements.sports.gouv.fr/api/records/1.0/search/?dataset=data-es&q=&refine.code_dept=69&rows=10000")
data = response.json()
for record in data['records']:
    rc = record['fields']
    dataframe.loc[len(dataframe.index)] = [rc['nominstallation'],rc['nom_commune'],rc['typequipement'],rc['coordgpsx'],rc['coordgpsy']]

# Requete à l'API du Grand Lyon
response = rq.get("https://download.data.grandlyon.com/ws/rdata/urbalyon.recenseqptsport/all.json?maxfeatures=10000&start=1")
data = response.json()
for record in data['values']: 
    dataframe.loc[len(dataframe.index)] = [record['nom'],record['commune'],record['type'],record['lon'],record['lat']]

# Traitement des données
dataframe = dataframe.drop_duplicates() # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html
category_map = CategoryMap.categories
dataframe['Category'] = dataframe['Type'].map(category_map)

# Export en csv
dataframe.to_csv('activities2.csv')
