from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType,DateType
from pyspark.sql.functions import expr

#mongodb uri and dbname , collection_name
mongo_uri = "mongodb://localhost:27017/" 
mongo_db_name = "MUSIC_App"
collection_artists = "artists"


if __name__ == "__main__":
    # Initialize Spark session with Kafka dependencies
    spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.output.uri", mongo_uri) \
        .config("spark.mongodb.output.database", mongo_db_name) \
        .config("spark.mongodb.output.collection", collection_artists) \
        .getOrCreate()
# Define the structure for the User data

    # Define the structure for the Artist data
    artist_schema = StructType([
        StructField("name", StringType(), True),
        StructField("listeners",IntegerType(), True),
        StructField("total_playcount",IntegerType(), True),
        StructField("artist_url", StringType(), True),
        StructField("bio", StringType(), True),
        StructField("artist_image", StringType(), True),
        StructField("albums", ArrayType(
            StructType([
                StructField("name", StringType(), True),
                StructField("playcount", IntegerType(), True),
                StructField("album_url", StringType(), True),
                StructField("album_image", StringType(), True),
                StructField("album_details", StructType([
                    StructField("wiki", StringType(), True),
                ]), True),
            ])
        ), True),
        StructField("genre", StringType(), True),
        StructField("country", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("top_tags", ArrayType(StringType()), True),
        StructField("id", IntegerType(), True),
    ])

    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    artist_topic = "artists_topic"


    # Read artist data from Kafka
    artist_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", artist_topic) \
        .load()

    # Deserialize JSON data from Kafka
    artist_stream_df = artist_stream_df.selectExpr("CAST(value AS STRING)").select(from_json("value", artist_schema).alias("data")).select("data.*")


# Transformation for user data

# Transformation for artist data
artist_stream_df_transformed = artist_stream_df \
    .withColumn("bio", col("bio").rlike("<.*?>")) \
    .withColumn("artist_details_wiki", expr("FILTER(albums.album_details.wiki, wiki -> rlike(wiki, '<.*?>'))"))


# Print statements for debugging
print("Listening to Kafka topic:", artist_topic)


artist_stream_query = artist_stream_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batchDF, batchId: batchDF.write \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db_name) \
        .option("collection", collection_artists) \
        .mode("append") \
        .save()
    ) \
    .start()



query_artists = artist_stream_df.writeStream.outputMode("append").format("console").start()


query_artists.awaitTermination()
