"""BATCH LAYER : HDFS (master dataset) -> agrégats journaliers -> Cassandra.batch_view
Recalcul complet et idempotent (upsert) ; écrit aussi le watermark."""
from pyspark.sql import functions as F

from common import RAW_PATH, get_spark, write_cassandra

spark = get_spark("batch-layer")
raw = spark.read.parquet(RAW_PATH)

daily = (raw.groupBy("vehicle_id", "day")
         .agg(F.count("*").alias("events"),
              F.sum("is_alert").cast("long").alias("alerts"),
              F.avg("speed_kmh").alias("avg_speed"),
              F.max("speed_kmh").alias("max_speed"),
              F.avg("engine_temp_c").alias("avg_engine_temp"),
              F.max("engine_temp_c").alias("max_engine_temp"),
              F.min("fuel_level_pct").alias("min_fuel"),
              F.avg("tire_pressure_psi").alias("avg_tire_pressure"),
              (F.max("odometer_km") - F.min("odometer_km")).alias("distance_km"),
              F.sum(F.when(F.col("speed_kmh") > 120, 1).otherwise(0)).cast("long").alias("overspeed_events"))
         .withColumn("health_score",
                     F.greatest(F.lit(0.0), F.lit(100.0) - F.col("alerts") / F.col("events") * 100.0)))

write_cassandra(daily, "batch_view")

watermark = raw.agg(F.max("ts")).collect()[0][0]
if watermark is not None:
    meta = spark.createDataFrame([("watermark", watermark)], "key string, value timestamp")
    write_cassandra(meta, "batch_meta")

print(f"[batch] done, watermark={watermark}, rows={daily.count()}")
spark.stop()
