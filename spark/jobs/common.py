import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle.telemetry")
HDFS = os.getenv("HDFS_URL", "hdfs://namenode:8020")
RAW_PATH = f"{HDFS}/lambda/raw/telemetry"
CHECKPOINTS = f"{HDFS}/lambda/checkpoints"
KEYSPACE = "fleet"

EVENT_SCHEMA = StructType([
    StructField("vehicle_id", StringType()),
    StructField("ts", StringType()),
    StructField("speed_kmh", DoubleType()),
    StructField("rpm", DoubleType()),
    StructField("engine_temp_c", DoubleType()),
    StructField("fuel_level_pct", DoubleType()),
    StructField("tire_pressure_psi", DoubleType()),
    StructField("battery_v", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("odometer_km", DoubleType()),
    StructField("status", StringType()),
])


def get_spark(name):
    spark = (SparkSession.builder.appName(name)
             .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark, starting="latest"):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", TOPIC)
            .option("startingOffsets", starting)
            .option("failOnDataLoss", "false")
            .load())


def parse_events(kafka_df):
    return (kafka_df
            .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
            .select("e.*")
            .withColumn("ts", F.to_timestamp("ts"))
            .withColumn("is_alert", F.when(F.col("status") == "OK", 0).otherwise(1))
            .filter(F.col("vehicle_id").isNotNull() & F.col("ts").isNotNull()))


def write_cassandra(df, table, mode="append"):
    (df.write.format("org.apache.spark.sql.cassandra")
       .options(keyspace=KEYSPACE, table=table)
       .mode(mode).save())
