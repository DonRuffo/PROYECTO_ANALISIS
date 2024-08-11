import pandas as pd
import pyodbc as py
import sqlalchemy as db
from pandas.io import sql
from scipy import stats
import pymysql
from datetime import datetime, date
import pymongo as pm


#Conexión a SQL Server
con=db.create_engine('mssql+pyodbc://@localhost:1433/ANALISIS_DATOS?'+
                     'driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')


#extracción de de los datasets
fifa=pd.read_csv('fifa_ranking-2023-07-20.csv/fifa_ranking-2023-07-20.csv')
juegos=pd.read_csv('athletes.csv/athletes.csv')
covid=pd.read_csv('CovidDeaths.csv/CovidDeaths.csv')
bbc=pd.read_csv('bbc_news.csv')
pizza=pd.read_csv('pizza_sales.csv/pizza_sales.csv')
concerts=pd.read_csv('concerts.csv')
food=pd.read_csv('FastFoodNutritionMenuV3.csv')
huff=pd.read_csv('HuffPost.csv')
phish=pd.read_csv('phish.csv')
spotify=pd.read_csv('Spotify_songs/Spotify_songs.csv')


#verificación de la integridad de los datos y limpieza
fifa.isnull().sum() #limpiado
juegos.isnull().sum()
juegos_rellenados=juegos.fillna("0")
juegos_rellenados.isnull().sum() #limpiado
covid.isnull().sum()
covid_rellenado=covid.fillna("0")
covid_rellenado.isnull().sum() #limpiado
bbc.isnull().sum() #limpiado
pizza.isnull().sum() #limpiado
concerts.isnull().sum()
concerts_rellenado=concerts.fillna("0")
concerts_rellenado.isnull().sum() #limpiado
food.isnull().sum()
fooo_rellenado=food.fillna("0")
fooo_rellenado.isnull().sum() #limpiado
huff.isnull().sum()
huff_rellenado=huff.fillna("0")
huff_rellenado.isnull().sum() #limpiado
phish.isnull().sum()
phish_rellenado=phish.fillna("0")
phish_rellenado.isnull().sum() #limpiado
spotify.isnull().sum()
spotify_rellenado=spotify.fillna("0")
spotify_rellenado.isnull().sum() #limpiado


#envío de los datasets como tablas a la base de datos definida en la conexión con SQL Server
fifa.to_sql('fifa_rankings', con, index=False)
juegos_rellenados.to_sql('juegos_olimpicos', con, index=False)
covid_rellenado.to_sql('Covid_19', con, index=False)
bbc.to_sql('BBC_News', con, index=False)
pizza.to_sql('Dominios_Pizza', con, index=False)
concerts_rellenado.to_sql('Orquesta_de_NY', con, index=False)
fooo_rellenado.to_sql('Comida_rapida_restaurantes', con, index=False)
huff_rellenado.to_sql('HuffPots', con, index=False)
phish_rellenado.to_sql('Phish_Banda', con, index=False)
spotify_rellenado.to_sql('Spotify_songs', con, index=False)


#conexión y exportación de datos a MYSQL 
fifa
database_username='root'
database_password='dataDennis'
database_ip='localhost'
database_name='analisis_datos'
database_connection=db.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
conexion=database_connection.connect()
metadata=db.MetaData()

fifa.to_sql('fifa_rankings', con=conexion, index=False)
juegos_rellenados.to_sql('juegos_olimpicos',  con=conexion, index=False)
covid_rellenado.to_sql('Covid_19',  con=conexion, index=False)
bbc.to_sql('BBC_News',  con=conexion, index=False)
pizza.to_sql('Dominios_Pizza',  con=conexion, index=False)
concerts_rellenado.to_sql('Orquesta de NY',  con=conexion, index=False)
fooo_rellenado.to_sql('Comida_rapida_restaurantes',  con=conexion, index=False)
huff_rellenado.to_sql('HuffPots',  con=conexion, index=False)
phish_rellenado.to_sql('Phish_Banda', con=conexion, index=False)
spotify_rellenado.to_sql('Spotify_songs', con=conexion, index=False)

#exportación de datos a MONGODB 
cliente=pm.MongoClient('mongodb://localhost:27017')
db=cliente['DATALAKE']
df1=pd.DataFrame(fifa)
df2=pd.DataFrame(juegos_rellenados)
df3=pd.DataFrame(covid_rellenado)
df4=pd.DataFrame(bbc)
df5=pd.DataFrame(pizza)
df6=pd.DataFrame(concerts_rellenado)
df7=pd.DataFrame(fooo_rellenado)
df8=pd.DataFrame(huff_rellenado)
df9=pd.DataFrame(phish_rellenado)
df10=pd.DataFrame(spotify_rellenado)

listaDATOS=[df1, df2, df3, df4, df5, df6, df7, df8, df9, df10]
listaCOLECCIONES=['ranking_fifa', 'juegos_olimpicos', 'covid_19', 'BBC_News', 'Domino´s pizza', 'Orquesta_NY', 'Comida_rapida', 'HuffPosts', 'Banda:Phish', 'Canciones_Spotify']
for coleccion in listaCOLECCIONES:
    colec=db[coleccion]
    data=listaDATOS[0].to_dict(orient="records")
    colec.insert_many(data)
    listaDATOS.pop(0)


#importacion de los datos desde MongoDB
for colec in listaCOLECCIONES:
    datos=db[colec].find()
    
    frame=pd.DataFrame(list(datos))
    
    creacion=f'{colec}.csv'
    frame.to_csv(creacion, index=False)

#importacion de datos desde MySQL

tablaBBC=pd.read_sql("SELECT * FROM bbc_new", con)
tablaFIFA=pd.read_sql("SELECT * FROM fifa_rankings", con)
tablaHUFF=pd.read_sql("SELECT * FROM huffpots", con)
tablaCOVID=pd.read_sql("SELECT * FROM covid_19", con)
tablaCOMIDA=pd.read_sql("SELECT * FROM comida_rapida_restaurantes", con)
tablaPIZZA=pd.read_sql("SELECT * FROM dominios_pizza", con)
tablaJUEGOS=pd.read_sql("SELECT *FROM juegos_olimpicos", con)
tablaORQUESTA=pd.read_sql("SELECT * FROM orquesta_de_ny", con)
tablaPHISH=pd.read_sql("SELECT * FROM phish_banda", con)
tablaSPOTIFY=pd.read_sql("SELECT * FROM spotify_songs", con)
