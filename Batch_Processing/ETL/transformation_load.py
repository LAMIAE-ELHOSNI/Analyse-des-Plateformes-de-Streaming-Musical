from datetime import datetime, timedelta
import pandas as pd
import pyodbc

# SQL Server configuration
SQL_SERVER_CONFIG = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=datawarehouse_music;"
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

    # Generate unique Gender and assign IDs
    unique_Gender = users_data['Gender'].unique()
    Gender_id_mapping = {}
    for index, Gender in enumerate(unique_Gender):
        Gender_id_mapping[Gender] = index + 1    
    
    # Generate unique Spotify usage periods and assign IDs
    unique_usage_periods = users_data['spotify_usage_period'].unique()
    usage_period_id_mapping = {}
    for index, usage_period in enumerate(unique_usage_periods):
        usage_period_id_mapping[usage_period] = index + 1
    
    # Generate unique Spotify listening devices and assign IDs
    unique_listening_devices = users_data['spotify_listening_device'].unique()
    listening_device_id_mapping = {}
    for index, listening_device in enumerate(unique_listening_devices):
        listening_device_id_mapping[listening_device] = index + 1
    
    # Generate unique Spotify subscription plans and assign IDs
    unique_subscription_plans = users_data['spotify_subscription_plan'].unique()
    subscription_plan_id_mapping = {}
    for index, subscription_plan in enumerate(unique_subscription_plans):
        subscription_plan_id_mapping[subscription_plan] = index + 1
    
    # Generate unique premium subscription willingness values and assign IDs
    unique_willingness_values = users_data['premium_sub_willingness'].unique()
    willingness_id_mapping = {}
    for index, willingness in enumerate(unique_willingness_values):
        willingness_id_mapping[willingness] = index + 1
    
    # Generate unique preferred premium plans and assign IDs
    unique_premium_plans = users_data['preffered_premium_plan'].unique()
    premium_plan_id_mapping = {}
    for index, premium_plan in enumerate(unique_premium_plans):
        premium_plan_id_mapping[premium_plan] = index + 1
    
    # Generate unique favorite music genres and assign IDs
    # unique_genres = users_data['fav_music_genre'].unique()
    # genre_id_mapping = {}
    # for index, genre in enumerate(unique_genres):
    #     genre_id_mapping[genre] = index + 1
    
    # Generate unique music time slots and assign IDs
    unique_time_slots = users_data['music_time_slot'].unique()
    time_slot_id_mapping = {}
    for index, time_slot in enumerate(unique_time_slots):
        time_slot_id_mapping[time_slot] = index + 1
    
    # Generate unique music listening frequencies and assign IDs
    unique_lis_frequencies = users_data['music_lis_frequency'].unique()
    lis_frequency_id_mapping = {}
    for index, lis_frequency in enumerate(unique_lis_frequencies):
        lis_frequency_id_mapping[lis_frequency] = index + 1
    
    # Generate unique music exploration methods and assign IDs
    unique_expl_methods = users_data['music_expl_method'].unique()
    expl_method_id_mapping = {}
    for index, expl_method in enumerate(unique_expl_methods):
        expl_method_id_mapping[expl_method] = index + 1
    
    # Generate unique podcast listening frequencies and assign IDs
    unique_pod_lis_frequencies = users_data['pod_lis_frequency'].unique()
    pod_lis_frequency_id_mapping = {}
    for index, pod_lis_frequency in enumerate(unique_pod_lis_frequencies):
        pod_lis_frequency_id_mapping[pod_lis_frequency] = index + 1
    
    # Generate unique favorite podcast genres and assign IDs
    unique_pod_genres = users_data['fav_pod_genre'].unique()
    pod_genre_id_mapping = {}
    for index, pod_genre in enumerate(unique_pod_genres):
        pod_genre_id_mapping[pod_genre] = index + 1
    
    # Generate unique preferred podcast formats and assign IDs
    unique_pod_formats = users_data['preffered_pod_format'].unique()
    pod_format_id_mapping = {}
    for index, pod_format in enumerate(unique_pod_formats):
        pod_format_id_mapping[pod_format] = index + 1
    
    # Generate unique preferred podcast durations and assign IDs
    unique_pod_durations = users_data['preffered_pod_duration'].unique()
    pod_duration_id_mapping = {}
    for index, pod_duration in enumerate(unique_pod_durations):
        pod_duration_id_mapping[pod_duration] = index + 1
    
    # Generate unique podcast variety satisfaction levels and assign IDs
    unique_satisfactions = users_data['pod_variety_satisfaction'].unique()
    satisfaction_id_mapping = {}
    for index, satisfaction in enumerate(unique_satisfactions):
        satisfaction_id_mapping[satisfaction] = index + 1
    
    # Insert unique nationalities into the countries_user table if they don't exist
    for nationality, nationality_id in nationality_id_mapping.items():
        cursor.execute("INSERT INTO nationality (id, nationality) VALUES (?, ?)", (int(nationality_id), str(nationality)))
    
    # Insert unique Genders into the gender_user table if they don't exist
    for Gender, Gender_id in Gender_id_mapping.items():
        cursor.execute("INSERT INTO gender_user (id, gender) VALUES (?, ?)", (int(Gender_id), str(Gender)))
    
    # Insert unique Spotify usage periods into the usage_period table if they don't exist
    for usage_period, usage_period_id in usage_period_id_mapping.items():
        cursor.execute("INSERT INTO usage_period (id, period) VALUES (?, ?)", (int(usage_period_id), str(usage_period)))
    
    # Insert unique Spotify listening devices into the listening_device table if they don't exist
    for listening_device, listening_device_id in listening_device_id_mapping.items():
        cursor.execute("INSERT INTO listening_device (id, device) VALUES (?, ?)", (int(listening_device_id), str(listening_device)))
    
    # Insert unique Spotify subscription plans into the subscription_plan table if they don't exist
    for subscription_plan, subscription_plan_id in subscription_plan_id_mapping.items():
        cursor.execute("INSERT INTO subscription_plan (id, [plan]) VALUES (?, ?)", (int(subscription_plan_id), str(subscription_plan)))
    
    # Insert unique premium subscription willingness values into the sub_willingness table if they don't exist
    for willingness, willingness_id in willingness_id_mapping.items():
        cursor.execute("INSERT INTO sub_willingness (id, willingness) VALUES (?, ?)", (int(willingness_id), str(willingness)))
    
    # # Insert unique preferred premium plans into the premium_plan table if they don't exist
    # for premium_plan, premium_plan_id in premium_plan_id_mapping.items():
    #     cursor.execute("INSERT INTO premium_plan (id, premium_plan) VALUES (?, ?)", (premium_plan_id, premium_plan))
    
    # Insert unique preferred listening content into the listening_content table if they don't exist

    # # Insert unique favorite music genres into the music_genre table if they don't exist
    # for genre, genre_id in genre_id_mapping.items():
    #     cursor.execute("INSERT INTO music_genre (id, genre) VALUES (?, ?)", (int(genre_id),str(genre)))
    
    # Merge users_data with genres on the fav_music_genre column
    users_data = users_data.merge(genres, how='left', left_on='fav_music_genre', right_on='genre')
    users_data['id_genre'].fillna(206, inplace=True)
    # Insert unique music time slots into the time_slot table if they don't exist
    for time_slot, time_slot_id in time_slot_id_mapping.items():
        cursor.execute("INSERT INTO time_slot (id, time_slot) VALUES (?, ?)", (int(time_slot_id), str(time_slot)))
    
    # Insert unique music listening frequencies into the lis_frequency table if they don't exist
    for lis_frequency, lis_frequency_id in lis_frequency_id_mapping.items():
        cursor.execute("INSERT INTO lis_frequency (id, frequency) VALUES (?, ?)", (int(lis_frequency_id), str(lis_frequency)))
    
    # Insert unique music exploration methods into the expl_method table if they don't exist
    for expl_method, expl_method_id in expl_method_id_mapping.items():
        cursor.execute("INSERT INTO expl_method (id, method) VALUES (?, ?)", (int(expl_method_id), str(expl_method)))
    
    # Insert unique podcast listening frequencies into the pod_lis_frequency table if they don't exist
    for pod_lis_frequency, pod_lis_frequency_id in pod_lis_frequency_id_mapping.items():
        cursor.execute("INSERT INTO pod_lis_frequency (id, frequency) VALUES (?, ?)", (int(pod_lis_frequency_id), str(pod_lis_frequency)))
    
    # Insert unique favorite podcast genres into the pod_genre table if they don't exist
    for pod_genre, pod_genre_id in pod_genre_id_mapping.items():
        cursor.execute("INSERT INTO pod_genre (id, genre) VALUES (?, ?)", (int(pod_genre_id), str(pod_genre)))
    
    # Insert unique preferred podcast formats into the pod_format table if they don't exist
    for pod_format, pod_format_id in pod_format_id_mapping.items():
        cursor.execute("INSERT INTO pod_format (id, format) VALUES (?, ?)", (int(pod_format_id), str(pod_format)))
    
    # Insert unique preferred podcast durations into the pod_duration table if they don't exist
    for pod_duration, pod_duration_id in pod_duration_id_mapping.items():
        cursor.execute("INSERT INTO pod_duration (id, duration) VALUES (?, ?)", (int(pod_duration_id), str(pod_duration)))
    
    # Insert unique podcast variety satisfaction levels into the pod_variety_satisfaction table if they don't exist
    for satisfaction, satisfaction_id in satisfaction_id_mapping.items():
        cursor.execute("INSERT INTO pod_variety_satisfaction (id, satisfaction) VALUES (?, ?)", (int(satisfaction_id), str(satisfaction)))
    
    # Update nationality, Gender, and other column IDs in users_data DataFrame with corresponding IDs
    users_data['nationality_id'] = users_data['nationality'].map(nationality_id_mapping)
    users_data['Gender_id'] = users_data['Gender'].map(Gender_id_mapping)
    users_data['spotify_usage_period_id'] = users_data['spotify_usage_period'].map(usage_period_id_mapping)
    users_data['spotify_listening_device_id'] = users_data['spotify_listening_device'].map(listening_device_id_mapping)
    users_data['spotify_subscription_plan_id'] = users_data['spotify_subscription_plan'].map(subscription_plan_id_mapping)
    users_data['premium_sub_willingness_id'] = users_data['premium_sub_willingness'].map(willingness_id_mapping)
    # users_data['preffered_premium_plan_id'] = users_data['preffered_premium_plan'].map(premium_plan_id_mapping)
    # users_data['fav_music_genre_id'] = users_data['fav_music_genre'].map(genre_id_mapping)
    users_data['music_time_slot_id'] = users_data['music_time_slot'].map(time_slot_id_mapping)
    users_data['music_lis_frequency_id'] = users_data['music_lis_frequency'].map(lis_frequency_id_mapping)
    users_data['music_expl_method_id'] = users_data['music_expl_method'].map(expl_method_id_mapping)
    users_data['pod_lis_frequency_id'] = users_data['pod_lis_frequency'].map(pod_lis_frequency_id_mapping)
    users_data['fav_pod_genre_id'] = users_data['fav_pod_genre'].map(pod_genre_id_mapping)
    users_data['preffered_pod_format_id'] = users_data['preffered_pod_format'].map(pod_format_id_mapping)
    users_data['preffered_pod_duration_id'] = users_data['preffered_pod_duration'].map(pod_duration_id_mapping)
    users_data['pod_variety_satisfaction_id'] = users_data['pod_variety_satisfaction'].map(satisfaction_id_mapping)
    
 # Update nationality, Gender, and other column IDs in users_data DataFrame with corresponding IDs
    users_data['nationality_id'] = users_data['nationality_id'].astype(int)
    users_data['Gender_id'] = users_data['Gender_id'].astype(int)
    users_data['spotify_usage_period_id'] = users_data['spotify_usage_period_id'].astype(int)
    users_data['spotify_listening_device_id'] = users_data['spotify_listening_device_id'].astype(int)
    users_data['spotify_subscription_plan_id'] = users_data['spotify_subscription_plan_id'].astype(int)
    users_data['premium_sub_willingness_id'] = users_data['premium_sub_willingness_id'].astype(int)
    # users_data['preffered_premium_plan_id'] = users_data['preffered_premium_plan'].map(premium_plan_id_mapping)
    # users_data['fav_music_genre_id'] = users_data['fav_music_genre_id'].astype(int)
    users_data['music_time_slot_id'] = users_data['music_time_slot_id'].astype(int)
    users_data['music_lis_frequency_id'] = users_data['music_lis_frequency_id'].astype(int)
    users_data['music_expl_method_id'] = users_data['music_expl_method_id'].astype(int)
    users_data['pod_lis_frequency_id'] = users_data['pod_lis_frequency_id'].astype(int)
    users_data['fav_pod_genre_id'] = users_data['fav_pod_genre_id'].astype(int)
    users_data['preffered_pod_format_id'] = users_data['preffered_pod_format_id'].astype(int)
    users_data['preffered_pod_duration_id'] = users_data['preffered_pod_duration_id'].astype(int)
    users_data['pod_variety_satisfaction_id'] = users_data['pod_variety_satisfaction_id'].astype(int)
    users_data['id'] = users_data['id'].astype(int)
    users_data['Age'] = users_data['Age'].astype(int)
    users_data['music_recc_rating'] = users_data['music_recc_rating'].astype(int)
# Convert the date_registration column to datetime
    users_data['date_registration'] = pd.to_datetime(users_data['date_registration'])

    for index, row in users_data.iterrows():
        # Replace the table name 'user' with '[user]' to properly escape it
        cursor.execute("INSERT INTO [users_data] (id, Age, Gender, spotify_usage_period, spotify_listening_device, spotify_subscription_plan, "
                "premium_sub_willingness, fav_music_genre_id, music_time_slot, "
                "music_lis_frequency, music_expl_method, music_recc_rating, pod_lis_frequency, fav_pod_genre, preffered_pod_format, "
                "preffered_pod_duration, pod_variety_satisfaction, date_registration, nationality_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (row['id'], row['Age'], row['Gender_id'], row['spotify_usage_period_id'], row['spotify_listening_device_id'], 
                row['spotify_subscription_plan_id'], row['premium_sub_willingness_id'], 
                row['id_genre'], row['music_time_slot_id'], row['music_lis_frequency_id'], 
                row['music_expl_method_id'], row['music_recc_rating'], row['pod_lis_frequency_id'], row['fav_pod_genre_id'], 
                row['preffered_pod_format_id'], row['preffered_pod_duration_id'], row['pod_variety_satisfaction_id'], 
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
