import json
from pymongo import MongoClient

# MongoDB connection string without username and password
# Replace 'localhost' with your MongoDB server
connection_string = 'mongodb://localhost:27017/'

# JSON file path
json_file_path = r'C:\Users\Youcode\Desktop\Analyse_Des_Plateformes_De_Streaming_Musical\Data_Ingestion\DATA\artiste\artist_data.json'

# Database name in MongoDB
database_name = 'Artiste_Data'

# Collection name in MongoDB
collection_name = 'Artiste'

# Connect to MongoDB
client = MongoClient(connection_string)

# Check if the database exists
existing_databases = client.list_database_names()
if database_name not in existing_databases:
    # Create the database if it doesn't exist
    client[database_name].create_collection(collection_name)

# Connect to the specific database
db = client.get_database(database_name)

# Read JSON file
with open(json_file_path, 'r') as json_file:
    data = json.load(json_file)

# Insert data into MongoDB collection
db[collection_name].insert_many(data)

print(f"Data from {json_file_path} has been inserted into the {collection_name} collection in MongoDB.")
