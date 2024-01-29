from datetime import datetime, timedelta
import pandas as pd
import pyodbc

# SQL Server configuration
SQL_SERVER_CONFIG = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=datawarehouse;"
    "Trusted_Connection=yes;"
)

class LocalDataLakeStorage:
    def __init__(self, data_path):
        self.data_path = data_path

    def load_csv(self, file_name):
        return pd.read_csv(f"{self.data_path}/{file_name}")

    def load_json(self, file_name):
        return pd.read_json(f"{self.data_path}/{file_name}")

# Function to transform and load other tables
def transform_and_load_other_tables(ldls):
    # Load data from local data lake
    genres = ldls.load_json("genres.json")
    gender = ldls.load_json("gender.json")
    countries = ldls.load_json("countries.json")
    tags = ldls.load_json("Tags.json")
    artists = ldls.load_json("artists.json")
    artist_tags = ldls.load_json("artist_tags.json")
    albums_artists = ldls.load_json("albums_artists.json")

    # Establish connection to SQL Server
    connection = pyodbc.connect(SQL_SERVER_CONFIG)
    cursor = connection.cursor()

    # Insert data into genres table
    for genre in genres.itertuples():
        cursor.execute("INSERT INTO genres (id_genre, genre) VALUES (?, ?)", (int(genre.id_genre), str(genre.genre)))
    connection.commit()

    # Insert data into gender table
    for g in gender.itertuples():
        cursor.execute("INSERT INTO gender (id_gender, gender) VALUES (?, ?)", (int(g.id_gender), str(g.gender)))
    connection.commit()

    # Insert data into countries table
    for country in countries.itertuples():
        cursor.execute("INSERT INTO countries (id_country, country_code, country_name) VALUES (?, ?, ?)",
                       (int(country.id_country), str(country.country_code), str(country.country_name)))
    connection.commit()

    # Insert data into tags table
    for tag in tags.itertuples():
        cursor.execute("INSERT INTO tags (id_tag, tag) VALUES (?, ?)", (int(tag.id_tag), str(tag.tag)))
    connection.commit()

    # Insert data into artists table
    for artist in artists.itertuples():
        cursor.execute("INSERT INTO artists (artist_id, artist_name, listeners, total_playcount, genre_id, gender_id, country_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (int(artist.artist_id), str(artist.artist_name), int(artist.listeners), int(artist.total_playcount), int(artist.genre_id), int(artist.gender_id), int(artist.country_id)))
    connection.commit()

    # Insert data into artist_tags table
    for artist_tag in artist_tags.itertuples():
        cursor.execute("INSERT INTO artist_tags (id_artist_tag, id_tag, id_artist) VALUES (?, ?, ?)",
                       (int(artist_tag.id_artist_tag), int(artist_tag.id_tag), int(artist_tag.id_artist)))
    connection.commit()

    # Insert data into albums_artists table
    for album_artist in albums_artists.itertuples():
        cursor.execute("INSERT INTO albums_artists (album_id, artist_id, album_name, playcount) VALUES (?, ?, ?, ?)",
                       (int(album_artist.album_id), int(album_artist.artist_id), str(album_artist.album_name), int(album_artist.playcount)))
    connection.commit()

    # Close the connection
    connection.close()


def load_and_transform_users_data(ldls, cursor):
    genres = ldls.load_json("genres.json")
    users_data = ldls.load_csv("users_data.csv")
    # Load users_data CSV file into DataFrame from local data lake
   
    # Generate unique nationalities and assign IDs
    unique_nationalities = users_data['nationality'].unique()
    nationality_id_mapping = {}
    for index, nationality in enumerate(unique_nationalities):
        nationality_id_mapping[nationality] = index + 1

    # Insert unique nationalities into the countries_user table if they don't exist
    for nationality, nationality_id in nationality_id_mapping.items():
        cursor.execute("INSERT INTO countries_user (id_country, country_name) VALUES (?, ?)", (nationality_id, nationality))

    # Update nationality column in users_data DataFrame with nationality IDs
    users_data['nationality_id'] = users_data['nationality'].map(nationality_id_mapping)

    # Merge users_data with genres on the fav_music_genre column
    users_data = users_data.merge(genres, how='left', left_on='fav_music_genre', right_on='genre')
    # Convert 'date_registration' to datetime
    users_data['date_registration'] = pd.to_datetime(users_data['date_registration'])
    # Generate unique nationality IDs
    users_data['id'] = users_data['id'].astype(int)
    users_data['Age'] = users_data['Age'].astype(int)
    users_data['Gender'] = users_data['Gender'].astype(str)
    users_data['spotify_usage_period'] = users_data['spotify_usage_period'].astype(str)
    users_data['spotify_listening_device'] = users_data['spotify_listening_device'].astype(str)
    users_data['spotify_subscription_plan'] = users_data['spotify_subscription_plan'].astype(str)
    users_data['premium_sub_willingness'] = users_data['premium_sub_willingness'].astype(str)
    users_data['preffered_premium_plan'] = users_data['preffered_premium_plan'].astype(str)
    users_data['preferred_listening_content'] = users_data['preferred_listening_content'].astype(str)
    users_data['music_time_slot'] = users_data['music_time_slot'].astype(str)
    users_data['music_lis_frequency'] = users_data['music_lis_frequency'].astype(str)
    users_data['music_expl_method'] = users_data['music_expl_method'].astype(str)
    users_data['music_recc_rating'] = users_data['music_recc_rating'].astype(int)
    users_data['pod_lis_frequency'] = users_data['pod_lis_frequency'].astype(str)
    users_data['fav_pod_genre'] = users_data['fav_pod_genre'].astype(str)
    users_data['preffered_pod_format'] = users_data['preffered_pod_format'].astype(str)
    users_data['preffered_pod_duration'] = users_data['preffered_pod_duration'].astype(str)
    users_data['pod_variety_satisfaction'] = users_data['pod_variety_satisfaction'].astype(str)
    users_data['date_registration'] = pd.to_datetime(users_data['date_registration'])
    users_data['id_genre'].fillna(206, inplace=True)
    users_data['nationality_id'] = users_data['nationality_id'].astype(int)

    for index, row in users_data.iterrows():
        # Replace the table name 'user' with '[user]' to properly escape it
        cursor.execute("INSERT INTO [users_data] (id, Age, Gender, spotify_usage_period, spotify_listening_device, spotify_subscription_plan, "
                "premium_sub_willingness, preffered_premium_plan, preferred_listening_content, fav_music_genre_id, music_time_slot, "
                "music_lis_frequency, music_expl_method, music_recc_rating, pod_lis_frequency, fav_pod_genre, preffered_pod_format, "
                "preffered_pod_duration, pod_variety_satisfaction, date_registration, nationality_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (row['id'], row['Age'], row['Gender'], row['spotify_usage_period'], row['spotify_listening_device'], 
                row['spotify_subscription_plan'], row['premium_sub_willingness'], row['preffered_premium_plan'], 
                row['preferred_listening_content'], row['id_genre'], row['music_time_slot'], row['music_lis_frequency'], 
                row['music_expl_method'], row['music_recc_rating'], row['pod_lis_frequency'], row['fav_pod_genre'], 
                row['preffered_pod_format'], row['preffered_pod_duration'], row['pod_variety_satisfaction'], 
                row['date_registration'], row['nationality_id']))


def main():
    # Local Data Lake Storage configuration
    DATA_PATH = "C:/Users/Youcode/Desktop/Analyse_Des_Plateformes_De_Streaming_Musical/data_lake"
    ldls = LocalDataLakeStorage(DATA_PATH)
    connection = pyodbc.connect(SQL_SERVER_CONFIG)
    cursor = connection.cursor()

    # Execute transformation and loading for other tables
    transform_and_load_other_tables(ldls)
    
    load_and_transform_users_data(ldls, cursor)
    connection.commit()
    # Close connection
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
