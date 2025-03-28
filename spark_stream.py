import logging
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_keyspace(session):
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
            """
        )
        logger.info("Keyspace created successfully!")
    except Exception as e:
        logger.error(f"Error creating keyspace: {str(e)}")
        raise

def create_table(session):
    try:
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            );
            """
        )
        logger.info("Table created successfully!")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

def create_spark_connection():
    try:
        spark = (
            SparkSession.builder
            .appName('SparkDataStreaming')
            .config("spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
            .config("spark.kafka.bootstrap.servers", "broker:29092")  # Correct internal port
            .config("spark.driver.host", "spark-master")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully!")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def connect_to_kafka(spark):
    try:
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")  # Internal Docker port
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        logger.info("Kafka connection established successfully")
        return df
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {str(e)}")
        raise

def create_cassandra_connection():
    try:
        cluster = Cluster(
            ['cassandra'],
            port=9042,
            protocol_version=4,
            load_balancing_policy=DCAwareRoundRobinPolicy()
        )
        session = cluster.connect()
        logger.info("Connected to Cassandra cluster successfully")
        return session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def create_selection_df(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    return (
        spark_df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

if __name__ == "__main__":
    try:
        spark = create_spark_connection()
        kafka_df = connect_to_kafka(spark)
        selection_df = create_selection_df(kafka_df)
        cassandra_session = create_cassandra_connection()

        create_keyspace(cassandra_session)
        create_table(cassandra_session)

        logger.info("Starting streaming process...")
        query = (
            selection_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("keyspace", "spark_streams")
            .option("table", "created_users")
            .start()
        )

        query.awaitTermination()
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise