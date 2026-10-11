CREATE KEYSPACE IF NOT EXISTS fleet
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE fleet;

-- ALL-DATA-VIEW : chaque évènement brut (écrit par ingest.py, conservé 30 jours)
CREATE TABLE IF NOT EXISTS all_data_view (
  vehicle_id text,
  day date,
  ts timestamp,
  speed_kmh double,
  rpm double,
  engine_temp_c double,
  fuel_level_pct double,
  tire_pressure_psi double,
  battery_v double,
  lat double,
  lon double,
  odometer_km double,
  status text,
  is_alert int,
  PRIMARY KEY ((vehicle_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC) AND default_time_to_live = 2592000;

-- REAL-TIME-VIEW : agrégats par minute (speed layer), TTL 24 h
CREATE TABLE IF NOT EXISTS realtime_view (
  vehicle_id text,
  window_start timestamp,
  window_end timestamp,
  events bigint,
  alerts bigint,
  avg_speed double,
  max_speed double,
  avg_rpm double,
  avg_engine_temp double,
  max_engine_temp double,
  min_fuel double,
  PRIMARY KEY (vehicle_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC) AND default_time_to_live = 86400;

-- BATCH-VIEW : agrégats journaliers recalculés depuis HDFS (batch layer)
CREATE TABLE IF NOT EXISTS batch_view (
  vehicle_id text,
  day date,
  events bigint,
  alerts bigint,
  avg_speed double,
  max_speed double,
  avg_engine_temp double,
  max_engine_temp double,
  min_fuel double,
  avg_tire_pressure double,
  distance_km double,
  overspeed_events bigint,
  health_score double,
  PRIMARY KEY (vehicle_id, day)
);

-- Watermark : jusqu'où le batch a traité les données
CREATE TABLE IF NOT EXISTS batch_meta (
  key text PRIMARY KEY,
  value timestamp
);
