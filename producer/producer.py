"""Producteur Kafka : simulateur de flotte OU rejeu d'un CSV Kaggle.

Évènement canonique (JSON) envoyé sur le topic :
  vehicle_id, ts, speed_kmh, rpm, engine_temp_c, fuel_level_pct,
  tire_pressure_psi, battery_v, lat, lon, odometer_km, status (OK|WARNING|FAULT)

mapping.json : champ canonique -> nom de colonne du CSV (null si absent).
"""
import csv
import json
import math
import os
import random
import re
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle.telemetry")
MODE = os.getenv("MODE", "simulator")
N_VEHICLES = int(os.getenv("N_VEHICLES", "50"))
RATE = float(os.getenv("EVENTS_PER_SECOND", "20"))
CSV_PATH = os.getenv("CSV_PATH", "/data/vehicles.csv")
MAPPING_PATH = os.getenv("MAPPING_PATH", "mapping.json")
BASE_LAT = float(os.getenv("BASE_LAT", "48.8566"))
BASE_LON = float(os.getenv("BASE_LON", "2.3522"))
FIELDS = ["vehicle_id", "speed_kmh", "rpm", "engine_temp_c", "fuel_level_pct",
          "tire_pressure_psi", "battery_v", "lat", "lon", "odometer_km", "status"]


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def compute_status(e):
    t, f, p = e.get("engine_temp_c"), e.get("fuel_level_pct"), e.get("tire_pressure_psi")
    if t is not None and t > 110:
        return "FAULT"
    if (t is not None and t > 102) or (f is not None and f < 10) or (p is not None and p < 28):
        return "WARNING"
    return "OK"


class SimVehicle:
    def __init__(self, i):
        self.id = f"CAR-{i:04d}"
        self.speed = random.uniform(0, 80)
        self.temp = random.uniform(70, 90)
        self.fuel = random.uniform(30, 100)
        self.odo = random.uniform(5_000, 150_000)
        self.lat = BASE_LAT + random.uniform(-0.2, 0.2)
        self.lon = BASE_LON + random.uniform(-0.2, 0.2)
        self.heading = random.uniform(0, 2 * math.pi)
        self.tire = random.uniform(30, 35)
        self.hot = random.random() < 0.08  # quelques véhicules "fragiles"

    def step(self, dt=1.0):
        self.speed = min(max(self.speed + random.gauss(0, 6), 0), 150)
        rpm = 800 + self.speed * 32 + random.gauss(0, 120)
        target = 90 + self.speed * 0.12 + (14 if self.hot else 0)
        self.temp += (target - self.temp) * 0.05 + random.gauss(0, 0.6)
        dist = self.speed / 3600 * dt * 30  # temps accéléré x30
        self.odo += dist
        self.fuel -= dist * 0.07
        if self.fuel < 4:
            self.fuel = random.uniform(80, 100)  # plein
        self.heading += random.gauss(0, 0.15)
        self.lat += math.cos(self.heading) * dist / 111
        self.lon += math.sin(self.heading) * dist / 75
        self.tire = max(24, self.tire - random.random() * 0.002)
        e = {
            "vehicle_id": self.id, "ts": now_iso(),
            "speed_kmh": round(self.speed, 1), "rpm": round(rpm),
            "engine_temp_c": round(self.temp, 1), "fuel_level_pct": round(self.fuel, 1),
            "tire_pressure_psi": round(self.tire, 1),
            "battery_v": round(random.gauss(13.8, 0.25), 2),
            "lat": round(self.lat, 6), "lon": round(self.lon, 6),
            "odometer_km": round(self.odo, 2),
        }
        e["status"] = compute_status(e)
        return e


def simulator():
    fleet = [SimVehicle(i) for i in range(N_VEHICLES)]
    while True:
        yield random.choice(fleet).step()


def to_float(v):
    """Convertit 48,60%, 1009RPM, 48km/h, etc. en nombre flottant."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("%", "").replace(",", ".")
    s = re.sub(r"(?:km/h|RPM|[kK][pP]a|[cC]|g/s)$", "", s).strip()
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def csv_replay():
    with open(MAPPING_PATH) as f:
        mapping = json.load(f)
    loop = os.getenv("LOOP", "true").lower() == "true"
    while True:
        with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
            for i, row in enumerate(csv.DictReader(f)):
                e = {"ts": now_iso()}
                for field in FIELDS:
                    col = mapping.get(field)
                    raw = row.get(col) if col else None
                    if field == "vehicle_id":
                        e[field] = raw.strip() if raw else None
                    elif field == "status":
                        e[field] = raw.strip() if raw else None
                    else:
                        e[field] = to_float(raw)
                if not e["vehicle_id"]:
                    e["vehicle_id"] = f"CAR-{i % N_VEHICLES:04d}"
                if not e["status"]:
                    e["status"] = compute_status(e)
                yield e
        if not loop:
            return


def main():
    p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    gen = simulator() if MODE == "simulator" else csv_replay()
    print(f"[producer] mode={MODE} -> {BOOTSTRAP}/{TOPIC} @ {RATE}/s", flush=True)
    sent = 0
    for event in gen:
        p.produce(TOPIC, key=event["vehicle_id"], value=json.dumps(event))
        p.poll(0)
        sent += 1
        if sent % 500 == 0:
            print(f"[producer] {sent} events sent", flush=True)
        time.sleep(1.0 / RATE)
    p.flush()


if __name__ == "__main__":
    main()
