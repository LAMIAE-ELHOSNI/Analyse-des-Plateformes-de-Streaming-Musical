from elasticsearch import Elasticsearch
import json

# Initialize Elasticsearch client with the complete URL
es = Elasticsearch(['http://localhost:9200'])

# Function to index data from a JSON file into Elasticsearch
def index_data(json_file, index_name):
    with open(json_file) as f:
        data = json.load(f)
        for doc in data:
            es.index(index=index_name, body=doc)

pa1 = r'C:\Users\Youcode\Desktop\Analyse_Des_Plateformes_De_Streaming_Musical\repo\Analyse-des-Plateformes-de-Streaming-Musical\Data_Ingestion\DATA\artiste\artist_data_v.json'
pu2 = r'C:\Users\Youcode\Desktop\Analyse_Des_Plateformes_De_Streaming_Musical\repo\Analyse-des-Plateformes-de-Streaming-Musical\Data_Ingestion\DATA\user\users_data.json'

# Index data for artists
index_data(pa1, "artists")
print("Data inserted successfully into 'artists' index.")

# Index data for users
index_data(pu2, "users")
print("Data inserted successfully into 'users' index.")
