import pyodbc
import json
import csv
from azure.storage.blob import BlobServiceClient, BlobType
import time

# Define the list of table names in your database
TABLE_NAMES = ['gender', 'genres']

SQL_SERVER_CONFIG = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=LAPTOP-5VUQJT8J;"
    "DATABASE=artist_DB;"
    "Trusted_Connection=yes;"
)

JSON_FILE_PATH = r'C:\Users\Youcode\Desktop\Analyse_Des_Plateformes_De_Streaming_Musical\Data_Ingestion\DATA\review\reviews_users.json'
CSV_FILE_PATH = r'C:\Users\Youcode\Desktop\Analyse_Des_Plateformes_De_Streaming_Musical\Data_Ingestion\DATA\user\users_data.csv'

def extract_from_sql(table_name):
    connection = pyodbc.connect(SQL_SERVER_CONFIG)
    cursor = connection.cursor()

    # Extract data from the specified table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Convert rows to dictionaries
    data = []
    for row in rows:
        row_dict = dict(zip([column[0] for column in cursor.description], row))
        data.append(row_dict)

    connection.close()
    
    return data

# def extract_from_json():
#     with open(JSON_FILE_PATH, 'r') as file:
#         data = json.load(file)
#     return data

# def extract_from_csv():
#     data = []
#     with open(CSV_FILE_PATH, 'r') as file:
#         csv_reader = csv.reader(file)
#         for row in csv_reader:
#             data.append(row)
#     return data

def upload_to_adls(data, file_name, container_name, prefix=''):
    connection_string = "DefaultEndpointsProtocol=https;AccountName=sadatamusic;AccountKey=yLBRTIj83251G/+0UG5chvH+Acn0Wa4PaO0otQVfKdnsaRSZvAn96pC3bSpAiVUkcjPZJJ1hZXpW+AStcdvMSw==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(f'{prefix}/{file_name}')

    # Retry logic for uploading data
    retries = 3
    for _ in range(retries):
        try:
            # Upload the data
            blob_client.upload_blob(json.dumps(data), overwrite=True)
            break  # Upload successful, exit retry loop
        except Exception as e:
            if "Blob already exists" in str(e):
                # Blob already exists, delete it before uploading
                blob_client.delete_blob()
                continue
            else:
                print(f"Error uploading blob: {e}")
                # Sleep for a moment before retrying
                time.sleep(2)
                continue
    else:
        # Retry limit reached, handle accordingly
        print("Upload failed after retries")

def main():
    # Iterate over each table and extract data
    for table_name in TABLE_NAMES:
        table_data = extract_from_sql(table_name)
        upload_to_adls(table_data, f'{table_name}.json', 'datalaketestdag', 'data_lake')

    # # Extract data from JSON file
    # json_data = extract_from_json()
    # upload_to_adls(json_data, 'reviews_users.json', 'datalaketestdag', 'data_lake')

    # # Extract data from CSV file
    # csv_data = extract_from_csv()
    # upload_to_adls(csv_data, 'users_data.csv', 'datalaketestdag', 'data_lake')

if __name__ == "__main__":
    main()
