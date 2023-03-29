'''
ETL Projet Ydays 22-23 : OUALYON
'''

# Imports
import pandas as pd
import requests as rq
import json
import MapCategory as CategoryMap
import MapFiltreLyon as FiltreLyonMap

# Création du DataFrame
dataframe = pd.DataFrame(columns=['Nom', 'Commune','Adresse','Code postal','Type','Coord_X', 'Coord_Y'])

# Requete à l'API du gouvernement
response = rq.get("https://equipements.sports.gouv.fr/api/records/1.0/search/?dataset=data-es&q=&refine.code_dept=69&rows=10000&facet=adresse")
data = response.json()
for record in data['records']:
  rc = record['fields']
  if 'adresse' in rc:
    record_adresse = rc['adresse']
  else:
    record_adresse = str(rc['coordgpsy']) + '° N, ' + str(rc['coordgpsx']) + '° E'
  dataframe.loc[len(dataframe.index)] = [rc['nominstallation'],rc['nom_commune'],record_adresse,rc['codepostal'],rc['typequipement'],rc['coordgpsx'],rc['coordgpsy']]

# Requete à l'API du Grand Lyon
response = rq.get("https://download.data.grandlyon.com/ws/rdata/urbalyon.recenseqptsport/all.json?maxfeatures=10000&start=1")
data = response.json()
for record in data['values']: 
  if 'InsAdresse' in record['installation_details']:
    record_adresse = record['installation_details']['InsAdresse']
  else:
    record_adresse = str(record['lat']) + '° N, ' + str(record['lon']) + '° E'
  dataframe.loc[len(dataframe.index)] = [record['nom'],record['commune'],record_adresse,record['installation_details']['InsCodePostal'],record['type'],record['lon'],record['lat']]

# Traitement des données
## Suppression des doublons
dataframe.drop_duplicates(inplace=True)
dataframe.drop_duplicates(subset=['Type','Coord_X','Coord_Y'],inplace=True)
dataframe.drop_duplicates(subset=['Nom','Adresse','Code postal','Type'],inplace=True)

## Filtre sur Lyon
# Ajout d'une colonne "Commune = Lyon" avec des valeurs booléens  : True si la valeur de Commune est Lyon ef Nan sinon
FiltreLyonmap = FiltreLyonMap.filtreLyon
dataframe["IsLyon"] = dataframe["Commune"].map(FiltreLyonmap)
# On supprime la colonne qui a été ajouté "Commune=Lyon"
dataframe = dataframe[dataframe["IsLyon"] == True].copy()
dataframe = dataframe.drop(columns='IsLyon')

## Filtre sur les lieux publics
# On supprime les entrées contenant "college", "lycee", ...
dataframe = dataframe.loc[~dataframe['Nom'].str.contains(r'(college|collège|lycée|lycee|ecole)', case=False, na=False)]

## Filtre des valeurs aberantes hors des coordonées de Lyon (4.6 à 5.2 et 45.6 à 45.9)
dataframe = dataframe.loc[(dataframe['Coord_X'] >= 4.6) & (dataframe['Coord_X'] <= 5.2) & (dataframe['Coord_Y'] >= 45.6) & (dataframe['Coord_Y'] <= 45.9)]

## Catégorisation des sports
category_map = CategoryMap.categories
dataframe['Category'] = dataframe['Type'].map(category_map)

# Export en csv
dataframe.to_csv('activities.csv',sep=";")

# Export en GeoJSON

## Dico python vide :
GeoJson = {
  "type": "FeatureCollection",
  "features": []
}

## Alimentation des features
for index, row in dataframe.iterrows():
  feature = {
    "type": "Feature",
    "properties": {
      "Name": row["Nom"],
      "Commune" : row["Commune"],
      "Adresse" : row["Adresse"],
      "Code Postal" : row["Code postal"],
      "Type" : row["Type"],
      "Category" : row["Category"]
    },
    "geometry": {
      "coordinates": [
        row["Coord_X"],
        row["Coord_Y"]
      ],
      "type": "Point"
    }
  }
  GeoJson["features"].append(feature)

## convertion en JSON:
GeoJson = json.dumps(GeoJson)

## Création du fichier "GeoActivities.json"
with open("Activities.geojson", "w") as outfile:
  outfile.write(GeoJson)