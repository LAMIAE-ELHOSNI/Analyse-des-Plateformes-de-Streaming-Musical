from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType,DateType
from pyspark.sql.functions import expr



if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("KafkaSparkIntegration") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .getOrCreate()

    # Kafka configuration
    ekafka_bootstrap_servers = "localhost:9092"
    artist_topic = "artists_topic"
    artist_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ekafka_bootstrap_servers) \
        .option("subscribe", artist_topic) \
        .load()

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

    artist_stream_df = artist_stream_df.selectExpr("CAST(value AS STRING)").select(from_json("value", artist_schema).alias("data")).select("data.*")


# Transformation for user data

# Transformation for artist data
artist_stream_df_transformed = artist_stream_df \
    .withColumn("bio", col("bio").rlike("<.*?>")) \
    .withColumn("artist_details_wiki", expr("FILTER(albums.album_details.wiki, wiki -> rlike(wiki, '<.*?>'))"))


# Print statements for debugging
print("Listening to Kafka topic:", artist_stream_df)


query_user_stream = artist_stream_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "Artists") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "false") \
    .option("es.index.auto.create", "true") \
    .option("checkpointLocation", "./checkpointLocation/") \
    .start()

query_artists = artist_stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query_artists.awaitTermination()