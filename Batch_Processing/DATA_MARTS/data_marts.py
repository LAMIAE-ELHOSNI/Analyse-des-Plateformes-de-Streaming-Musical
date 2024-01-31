import pyodbc

# SQL Server connection configurations for data warehouse and data marts
data_warehouse_config = {
    "SERVER": "your_data_warehouse_server",
    "DATABASE": "your_data_warehouse_db",
    "Trusted_Connection": "yes"
}

data_mart_configs = {
    "ArtistData": {
        "SERVER": "localhost",
        "DATABASE": "artist_data_mart",
        "Trusted_Connection": "yes"
    },
    "UserData": {
        "SERVER": "localhost",
        "DATABASE": "user_data_mart",
        "Trusted_Connection": "yes"
    },
    "ReviewData": {
        "SERVER": "localhost",
        "DATABASE": "review_data_mart",
        "Trusted_Connection": "yes"
    }
}

# Function to execute SQL queries
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.commit()

# Function to create connection to a database
def create_connection(config):
    conn_str = ";".join([f"{key}={value}" for key, value in config.items()])
    return pyodbc.connect(conn_str)

# Data mart tables creation queries
data_mart_queries = {
    "ArtistData_Mart": [
        """
        CREATE TABLE IF NOT EXISTS genres (
            id_genre INT PRIMARY KEY,
            genre VARCHAR(MAX)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS gender (
            id_gender INT PRIMARY KEY,
            gender VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS countries (
            id_country INT PRIMARY KEY,
            country_code VARCHAR(10),
            country_name VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id INT PRIMARY KEY,
            artist_name VARCHAR(MAX),
            listeners INT,
            total_playcount INT,
            genre_id INT,
            gender_id INT,
            country_id INT,
            FOREIGN KEY (genre_id) REFERENCES genres(id_genre),
            FOREIGN KEY (gender_id) REFERENCES gender(id_gender),
            FOREIGN KEY (country_id) REFERENCES countries(id_country)
        )
        """
    ],
    "UserData": [
        """
        CREATE TABLE IF NOT EXISTS genres (
            id_genre INT PRIMARY KEY,
            genre VARCHAR(MAX)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS gender (
            id_gender INT PRIMARY KEY,
            gender VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS countries (
            id_country INT PRIMARY KEY,
            country_code VARCHAR(10),
            country_name VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users_data (
            id INT PRIMARY KEY,
            Age INT,
            Gender VARCHAR(20),
            spotify_usage_period VARCHAR(50),
            spotify_listening_device VARCHAR(100),
            spotify_subscription_plan VARCHAR(100),
            premium_sub_willingness VARCHAR(10),
            preffered_premium_plan VARCHAR(100),
            preferred_listening_content VARCHAR(100),
            fav_music_genre_id INT,
            music_time_slot VARCHAR(100),
            music_lis_frequency VARCHAR(100),
            music_expl_method VARCHAR(100),
            music_recc_rating INT,
            pod_lis_frequency VARCHAR(100),
            fav_pod_genre VARCHAR(100),
            preffered_pod_format VARCHAR(100),
            preffered_pod_duration VARCHAR(100),
            pod_variety_satisfaction VARCHAR(100),
            date_registration DATE,
            nationality_id INT,
            FOREIGN KEY (fav_music_genre_id) REFERENCES genres(id_genre),
            FOREIGN KEY (nationality_id) REFERENCES countries(id_country)
        )
        """
    ],
    "ReviewData": [
        """
        CREATE TABLE IF NOT EXISTS users_data (
            id INT PRIMARY KEY,
            Age INT,
            Gender VARCHAR(20),
            spotify_usage_period VARCHAR(50),
            spotify_listening_device VARCHAR(100),
            spotify_subscription_plan VARCHAR(100),
            premium_sub_willingness VARCHAR(10),
            preffered_premium_plan VARCHAR(100),
            preferred_listening_content VARCHAR(100),
            fav_music_genre_id INT,
            music_time_slot VARCHAR(100),
            music_lis_frequency VARCHAR(100),
            music_expl_method VARCHAR(100),
            music_recc_rating INT,
            pod_lis_frequency VARCHAR(100),
            fav_pod_genre VARCHAR(100),
            preffered_pod_format VARCHAR(100),
            preffered_pod_duration VARCHAR(100),
            pod_variety_satisfaction VARCHAR(100),
            date_registration DATE,
            nationality_id INT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS reviews (
            id INT PRIMARY KEY,
            Time_submitted DATETIME,
            Review TEXT,
            Rating INT,
            Total_thumbsup INT,
            date_registration DATE,
            user_id INT,
            FOREIGN KEY (user_id) REFERENCES users_data(id)
        )
        """
    ]
}

# Create and populate tables for each data mart
for data_mart, config in data_mart_configs.items():
    # Create connection to data mart
    mart_connection = create_connection(config)
    
    # Execute data mart tables creation queries
    for query in data_mart_queries[data_mart]:
        execute_query(mart_connection, query)
    
    # Close data mart connection
    mart_connection.close()
