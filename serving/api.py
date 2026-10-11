"""SERVING LAYER : fusionne batch_view (précis) + realtime_view (frais)."""
import os
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Fleet serving layer")
_session = None


def session():
    global _session
    if _session is None:
        _session = Cluster([os.getenv("CASSANDRA_HOST", "cassandra")]).connect("fleet")
    return _session


@app.get("/vehicles/{vehicle_id}/summary")
def summary(vehicle_id: str):
    s = session()
    wm_row = s.execute("SELECT value FROM batch_meta WHERE key='watermark'").one()
    wm = wm_row.value if wm_row else datetime(1970, 1, 1)

    batch = list(s.execute(
        "SELECT events, alerts, distance_km, max_speed, max_engine_temp, health_score "
        "FROM batch_view WHERE vehicle_id=%s", (vehicle_id,)))
    rt = list(s.execute(
        "SELECT events, alerts, max_speed, max_engine_temp FROM realtime_view "
        "WHERE vehicle_id=%s AND window_start >= %s", (vehicle_id, wm)))
    if not batch and not rt:
        raise HTTPException(404, "vehicle not found")

    maxes_speed = [r.max_speed for r in batch + rt if r.max_speed is not None]
    maxes_temp = [r.max_engine_temp for r in batch + rt if r.max_engine_temp is not None]
    return {
        "vehicle_id": vehicle_id,
        "batch_watermark": wm.isoformat(),
        "total_events": sum(r.events for r in batch) + sum(r.events for r in rt),
        "total_alerts": sum(r.alerts for r in batch) + sum(r.alerts for r in rt),
        "distance_km_batch": round(sum(r.distance_km or 0 for r in batch), 2),
        "max_speed": max(maxes_speed, default=None),
        "max_engine_temp": max(maxes_temp, default=None),
        "sources": {"batch_days": len(batch), "realtime_windows": len(rt)},
    }


@app.get("/vehicles/{vehicle_id}/latest")
def latest(vehicle_id: str):
    today = datetime.now(timezone.utc).date()
    row = session().execute(
        "SELECT * FROM all_data_view WHERE vehicle_id=%s AND day=%s LIMIT 1",
        (vehicle_id, today)).one()
    if not row:
        raise HTTPException(404, "no event today")
    return {k: (str(v) if k in ("day", "ts") else v) for k, v in row._asdict().items()}


@app.get("/realtime/top-alerts")
def top_alerts(limit: int = 10):
    rows = session().execute("SELECT vehicle_id, alerts, window_start FROM realtime_view LIMIT 2000")
    agg = {}
    for r in rows:
        agg[r.vehicle_id] = agg.get(r.vehicle_id, 0) + (r.alerts or 0)
    top = sorted(agg.items(), key=lambda x: -x[1])[:limit]
    return [{"vehicle_id": v, "alerts_recent": a} for v, a in top]
