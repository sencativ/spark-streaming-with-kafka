import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import from_json, col, sum, max, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = "game-voucher"

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
)

spark = (
    pyspark.sql.SparkSession.builder.appName("DibimbingStreaming")
    .master("local")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("game_title", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("ts", LongType(), True),
    ]
)

# Stream reading kafka
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Parsing JSON as STRING so Spark can process kafka data
parsed_df = (
    stream_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# convert timestamp for better readability purposes
parsed_df = parsed_df.withColumn("timestamp", to_timestamp("ts"))

# Adding watermark to handle late data, 10 minutes is the tolerance from timestamp
parsed_df_with_watermark = parsed_df.withWatermark("timestamp", "10 minutes")

# 10 minute window for aggregate
windowed_df = parsed_df_with_watermark.groupBy(
    window(col("timestamp"), "10 minutes")
).agg(
    sum("price").alias("Running_total"),
    max(col("timestamp")).alias("Timestamp"),
)

# Writing the result to the console every 30 seconds
query = (
    windowed_df.select("Timestamp", "Running_total")
    .writeStream.outputMode("complete")
    .format("console")
    .trigger(processingTime="30 seconds")
    .start()
)

# Await termination
query.awaitTermination()
