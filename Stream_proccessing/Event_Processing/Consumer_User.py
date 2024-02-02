from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaSparkIntegration") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .getOrCreate()

    # Kafka configuration
    elasticsearch_bootstrap_servers = "localhost:9092"
    user_topic = "users_topic"
    user_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", elasticsearch_bootstrap_servers) \
        .option("subscribe", user_topic) \
        .option("startingOffsets", "earliest") \
        .load()

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


    # Deserialize JSON data from Kafka
    user_stream_df = user_stream_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", user_schema).alias("data")) \
        .select("data.*")

    # Transformation for user data
    user_stream_df_transformed = user_stream_df.drop("uid")

    # Print statements for debugging
    print("Listening to Kafka topic:", user_topic)

    query_user_stream = user_stream_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.resource", "Usersindex") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "false") \
        .option("es.index.auto.create", "true") \
        .option("checkpointLocation", "./checkpointLocation/") \
        .start()

    query_users = user_stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query_users.awaitTermination()

