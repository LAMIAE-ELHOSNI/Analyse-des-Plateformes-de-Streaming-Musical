from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType,DateType

#mongodb uri and dbname , collection_name
mongo_uri = "mongodb://localhost:27017/" 
mongo_db_name = "MUSIC_App"
collection_users = "users"


if __name__ == "__main__":
    # Initialize Spark session with Kafka dependencies
    spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.output.uri", mongo_uri) \
        .config("spark.mongodb.output.database", mongo_db_name) \
        .config("spark.mongodb.output.collection", collection_users) \
        .getOrCreate()
# Define the structure for the User data
    user_schema = StructType([
        StructField("uid", StringType(), True),
        StructField("Age", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("spotify_usage_period", StringType(), True),
        StructField("spotify_listening_device", StringType(), True),
        StructField("spotify_subscription_plan", StringType(), True),
        StructField("premium_sub_willingness", StringType(), True),
        StructField("preffered_premium_plan", StringType(), True),
        StructField("preferred_listening_content", StringType(), True),
        StructField("fav_music_genre", ArrayType(StringType()), True),
        StructField("music_time_slot", StringType(), True),
        StructField("music_lis_frequency", StringType(), True),
        StructField("music_expl_method", StringType(), True),
        StructField("music_recc_rating", IntegerType(), True),
        StructField("pod_lis_frequency", StringType(), True),
        StructField("fav_pod_genre", StringType(), True),
        StructField("preffered_pod_format", StringType(), True),
        StructField("preffered_pod_duration", StringType(), True),
        StructField("pod_variety_satisfaction", StringType(), True),
        StructField("date_registration", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("id", IntegerType(), True),
    ])


    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    user_topic = "users_topic"
    # Read user data from Kafka
    user_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", user_topic) \
        .load()

    # Deserialize JSON data from Kafka
    user_stream_df = user_stream_df.selectExpr("CAST(value AS STRING)").select(from_json("value", user_schema).alias("data")).select("data.*")


# Transformation for user data
user_stream_df_transformed = user_stream_df.drop("uid")


# Print statements for debugging
print("Listening to Kafka topic:", user_topic)


user_stream_query = user_stream_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batchDF, batchId: batchDF.write \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db_name) \
        .option("collection", collection_users) \
        .mode("append") \
        .save()
    ) \
    .start()


query_users = user_stream_df.writeStream.outputMode("append").format("console").start()


query_users.awaitTermination()