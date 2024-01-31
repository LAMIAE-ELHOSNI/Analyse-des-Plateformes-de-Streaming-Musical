import pyodbc

# SQL Server configuration
SQL_SERVER_CONFIG = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=datawarehouse_music;"
    "Trusted_Connection=yes;"
)

# Schema for table creation
schemas = {
    "genres": {
        "id_genre": "INT PRIMARY KEY",
        "genre": "VARCHAR(MAX)"
    },
    "gender": {
        "id_gender": "INT PRIMARY KEY",
        "gender": "VARCHAR(255)"
    },
    "countries": {
        "id_country": "INT PRIMARY KEY",
        "country_code": "VARCHAR(10)",
        "country_name": "VARCHAR(255)"
    },
    "tags": {
        "id_tag": "INT PRIMARY KEY",
        "tag": "VARCHAR(MAX)"
    },
    "artists": {
        "artist_id": "INT PRIMARY KEY",
        "artist_name": "VARCHAR(MAX)",
        "listeners": "INT",
        "total_playcount": "INT",
        "genre_id": "INT",
        "gender_id": "INT",
        "country_id": "INT"
    },
    "artist_tags": {
        "id_artist_tag": "INT PRIMARY KEY",
        "id_tag": "INT",
        "id_artist": "INT"
    },
    "albums_artists": {
        "album_id": "INT PRIMARY KEY",
        "artist_id": "INT",
        "album_name": "VARCHAR(MAX)",
        "playcount": "INT"
    },
    "gender_user": {
        "id": "INT PRIMARY KEY",
        "gender": "VARCHAR(20)"
    },
    "usage_period": {
        "id": "INT PRIMARY KEY",
        "period": "VARCHAR(50)"
    },
    "listening_device": {
        "id": "INT PRIMARY KEY",
        "device": "VARCHAR(100)"
    },
    "subscription_plan": {
        "id": "INT PRIMARY KEY",
        "[plan]": "VARCHAR(100)"  # Enclose 'plan' in square brackets
    },
    "sub_willingness": {
        "id": "INT PRIMARY KEY",
        "willingness": "VARCHAR(10)"
    },
    "time_slot": {
        "id": "INT PRIMARY KEY",
        "time_slot": "VARCHAR(100)"
    },
    "lis_frequency": {
        "id": "INT PRIMARY KEY",
        "frequency": "VARCHAR(100)"
    },
    "expl_method": {
        "id": "INT PRIMARY KEY",
        "method": "VARCHAR(100)"
    },
    "pod_lis_frequency": {
        "id": "INT PRIMARY KEY",
        "frequency": "VARCHAR(100)"
    },
    "pod_genre": {
        "id": "INT PRIMARY KEY",
        "genre": "VARCHAR(100)"
    },
    "pod_format": {
        "id": "INT PRIMARY KEY",
        "format": "VARCHAR(100)"
    },
    "pod_duration": {
        "id": "INT PRIMARY KEY",
        "duration": "VARCHAR(100)"
    },
    "pod_variety_satisfaction": {
        "id": "INT PRIMARY KEY",
        "satisfaction": "VARCHAR(100)"
    },
    "nationality": {
        "id": "INT PRIMARY KEY",
        "nationality": "VARCHAR(100)"
    },
    "users_data": {
        "id": "INT PRIMARY KEY",
        "Age": "INT",
        "Gender": "INT",
        "spotify_usage_period": "INT",
        "spotify_listening_device": "INT",
        "spotify_subscription_plan": "INT",
        "premium_sub_willingness": "INT",
        "fav_music_genre_id": "INT",
        "music_time_slot": "INT",
        "music_lis_frequency": "INT",
        "music_expl_method": "INT",
        "music_recc_rating": "INT",
        "pod_lis_frequency": "INT",
        "fav_pod_genre": "INT",
        "preffered_pod_format": "INT",
        "preffered_pod_duration": "INT",
        "pod_variety_satisfaction": "INT",
        "date_registration": "DATE",
        "nationality_id": "INT"
    },
    "reviews": {
        "id": "INT PRIMARY KEY",
        "Time_submitted": "DATETIME",
        "Review": "TEXT",
        "Rating": "INT",
        "Total_thumbsup": "INT",
        "date_registration": "DATE",
        "user_id": "INT"
    }
}

# Foreign Key Constraints
# Foreign Key Constraints
foreign_keys = {
    "artists": {
        "artists_genre_id_constraint": "FOREIGN KEY (genre_id) REFERENCES genres(id_genre)",
        "artists_gender_id_constraint": "FOREIGN KEY (gender_id) REFERENCES gender(id_gender)",
        "artists_country_id_constraint": "FOREIGN KEY (country_id) REFERENCES countries(id_country)"
    },
    "artist_tags": {
        "artist_tags_id_tag_constraint": "FOREIGN KEY (id_tag) REFERENCES tags(id_tag)",
        "artist_tags_id_artist_constraint": "FOREIGN KEY (id_artist) REFERENCES artists(artist_id)"
    },
    "albums_artists": {
        "albums_artists_artist_id_constraint": "FOREIGN KEY (artist_id) REFERENCES artists(artist_id)"
    },
    "users_data": {
        "users_data_fav_music_genre_constraint": "FOREIGN KEY (fav_music_genre_id) REFERENCES genres(id_genre)",
        "users_data_nationality_id_constraint": "FOREIGN KEY (nationality_id) REFERENCES countries_user(id_country)",
        "users_data_Gender_constraint": "FOREIGN KEY (Gender) REFERENCES gender_user(id)",
        "users_data_spotify_usage_period_constraint": "FOREIGN KEY (spotify_usage_period) REFERENCES usage_period(id)",
        "users_data_spotify_listening_device_constraint": "FOREIGN KEY (spotify_listening_device) REFERENCES listening_device(id)",
        "users_data_spotify_subscription_plan_constraint": "FOREIGN KEY (spotify_subscription_plan) REFERENCES subscription_plan(id)",
        "users_data_premium_sub_willingness_constraint": "FOREIGN KEY (premium_sub_willingness) REFERENCES sub_willingness(id)",
        "users_data_music_time_slot_constraint": "FOREIGN KEY (music_time_slot) REFERENCES time_slot(id)",
        "users_data_music_lis_frequency_constraint": "FOREIGN KEY (music_lis_frequency) REFERENCES lis_frequency(id)",
        "users_data_music_expl_method_constraint": "FOREIGN KEY (music_expl_method) REFERENCES expl_method(id)",
        "users_data_pod_lis_frequency_constraint": "FOREIGN KEY (pod_lis_frequency) REFERENCES pod_lis_frequency(id)",
        "users_data_fav_pod_genre_constraint": "FOREIGN KEY (fav_pod_genre) REFERENCES pod_genre(id)",
        "users_data_preffered_pod_format_constraint": "FOREIGN KEY (preffered_pod_format) REFERENCES pod_format(id)",
        "users_data_preffered_pod_duration_constraint": "FOREIGN KEY (preffered_pod_duration) REFERENCES pod_duration(id)",
        "users_data_pod_variety_satisfaction_constraint": "FOREIGN KEY (pod_variety_satisfaction) REFERENCES pod_variety_satisfaction(id)",
        "users_data_nationality_id_constraint": "FOREIGN KEY (nationality_id) REFERENCES nationality(id)"
    },
    "reviews": {
        "reviews_user_id_constraint": "FOREIGN KEY (user_id) REFERENCES users_data(id)"
    }
}

# Function to create tables
def create_tables():
    connection = pyodbc.connect(SQL_SERVER_CONFIG)
    cursor = connection.cursor()

    for table_name, columns in schemas.items():
        create_table_query = f"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}') "
        create_table_query += f"CREATE TABLE {table_name} ({', '.join([f'{column} {data_type}' for column, data_type in columns.items()])})"
        cursor.execute(create_table_query)
        connection.commit()

    for table_name, keys in foreign_keys.items():
        for key_name, key_constraint in keys.items():
            # Check if the constraint already exists
            check_constraint_query = f"SELECT * FROM sys.foreign_keys WHERE name = '{key_name}' AND parent_object_id = OBJECT_ID('{table_name}')"
            cursor.execute(check_constraint_query)
            if not cursor.fetchone():
                alter_table_query = f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}') "
                alter_table_query += f"ALTER TABLE {table_name} ADD CONSTRAINT {key_name} {key_constraint}"
                cursor.execute(alter_table_query)
                connection.commit()

    connection.close()

# Create tables
create_tables()

