import pyodbc

# SQL Server configuration
SQL_SERVER_CONFIG = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=datawarehouse;"
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
    "countries_user": {
        "id_country": "INT PRIMARY KEY",
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
    "users_data": {
        "id": "INT PRIMARY KEY",
        "Age": "INT",
        "Gender": "VARCHAR(20)",
        "spotify_usage_period": "VARCHAR(50)",
        "spotify_listening_device": "VARCHAR(100)",
        "spotify_subscription_plan": "VARCHAR(100)",
        "premium_sub_willingness": "VARCHAR(10)",
        "preffered_premium_plan": "VARCHAR(100)",
        "preferred_listening_content": "VARCHAR(100)",
        "fav_music_genre_id": "INT",
        "music_time_slot": "VARCHAR(100)",
        "music_lis_frequency": "VARCHAR(100)",
        "music_expl_method": "VARCHAR(100)",
        "music_recc_rating": "INT",
        "pod_lis_frequency": "VARCHAR(100)",
        "fav_pod_genre": "VARCHAR(100)",
        "preffered_pod_format": "VARCHAR(100)",
        "preffered_pod_duration": "VARCHAR(100)",
        "pod_variety_satisfaction": "VARCHAR(100)",
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
foreign_keys = {
    "artists": {
        "genre_id_constraint": "FOREIGN KEY (genre_id) REFERENCES genres(id_genre)",
        "gender_id_constraint": "FOREIGN KEY (gender_id) REFERENCES gender(id_gender)",
        "country_id_constraint": "FOREIGN KEY (country_id) REFERENCES countries(id_country)"
    },
    "artist_tags": {
        "id_tag_constraint": "FOREIGN KEY (id_tag) REFERENCES tags(id_tag)",
        "id_artist_constraint": "FOREIGN KEY (id_artist) REFERENCES artists(artist_id)"
    },
    "albums_artists": {
        "artist_id_constraint": "FOREIGN KEY (artist_id) REFERENCES artists(artist_id)"
    },
    "users_data": {
        "fav_music_genre_constraint": "FOREIGN KEY (fav_music_genre_id) REFERENCES genres(id_genre)",
        "nationality_id_constraint": "FOREIGN KEY (nationality_id) REFERENCES countries_user(id_country)"
    },
    "reviews": {
        "user_id_constraint": "FOREIGN KEY (user_id) REFERENCES users_data(id)"
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
            alter_table_query = f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}') "
            alter_table_query += f"ALTER TABLE {table_name} ADD CONSTRAINT {key_name} {key_constraint}"
            cursor.execute(alter_table_query)
            connection.commit()

    connection.close()

# Create tables
create_tables()
