"""INGESTION : Kafka -> (1) HDFS parquet (master dataset du batch layer)
                      -> (2) Cassandra.all_data_view (évènements bruts)"""
from pyspark.sql import functions as F

from common import CHECKPOINTS, RAW_PATH, get_spark, parse_events, read_kafka, write_cassandra

spark = get_spark("ingest")
events = parse_events(read_kafka(spark, "earliest")).withColumn("day", F.to_date("ts"))

CASSANDRA_COLS = ["vehicle_id", "day", "ts", "speed_kmh", "rpm", "engine_temp_c",
                  "fuel_level_pct", "tire_pressure_psi", "battery_v", "lat", "lon",
                  "odometer_km", "status", "is_alert"]


def to_cassandra(batch_df, batch_id):
    write_cassandra(batch_df.select(*CASSANDRA_COLS), "all_data_view")


q_cassandra = (events.writeStream.foreachBatch(to_cassandra)
               .option("checkpointLocation", f"{CHECKPOINTS}/ingest_cassandra")
               .trigger(processingTime="5 seconds").start())

q_hdfs = (events.writeStream.format("parquet").outputMode("append")
          .option("path", RAW_PATH)
          .option("checkpointLocation", f"{CHECKPOINTS}/ingest_hdfs")
          .partitionBy("day")
          .trigger(processingTime="30 seconds").start())

spark.streams.awaitAnyTermination()
