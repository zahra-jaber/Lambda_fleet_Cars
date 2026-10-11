"""SPEED LAYER : Kafka -> agrégats par fenêtre de 1 min -> Cassandra.realtime_view"""
from pyspark.sql import functions as F

from common import CHECKPOINTS, get_spark, parse_events, read_kafka, write_cassandra

spark = get_spark("speed-layer")
events = parse_events(read_kafka(spark, "latest"))

agg = (events.withWatermark("ts", "2 minutes")
       .groupBy(F.window("ts", "1 minute").alias("w"), "vehicle_id")
       .agg(F.count("*").alias("events"),
            F.sum("is_alert").cast("long").alias("alerts"),
            F.avg("speed_kmh").alias("avg_speed"),
            F.max("speed_kmh").alias("max_speed"),
            F.avg("rpm").alias("avg_rpm"),
            F.avg("engine_temp_c").alias("avg_engine_temp"),
            F.max("engine_temp_c").alias("max_engine_temp"),
            F.min("fuel_level_pct").alias("min_fuel")))

out = agg.select("vehicle_id",
                 F.col("w.start").alias("window_start"),
                 F.col("w.end").alias("window_end"),
                 "events", "alerts", "avg_speed", "max_speed", "avg_rpm",
                 "avg_engine_temp", "max_engine_temp", "min_fuel")


def sink(batch_df, batch_id):
    # Cassandra = upsert : la même fenêtre mise à jour écrase l'ancienne valeur
    write_cassandra(batch_df, "realtime_view")


(out.writeStream.outputMode("update")
    .foreachBatch(sink)
    .option("checkpointLocation", f"{CHECKPOINTS}/speed")
    .trigger(processingTime="10 seconds")
    .start().awaitTermination())
