"""
thermal_agent.py
----------------
Run this on EVERY machine in the cluster (master AND client).
It reads real CPU temperature every 1 second and publishes
a JSON status blob to Redis under the key  node:status:<hostname>

The key expires after 5 seconds so dead nodes disappear automatically.
"""

import psutil
import redis
import time
import socket
import json

# ── CONFIG ───────────────────────────────────────────────────────────────────
MASTER_IP          = "100.89.81.24"   # <-- your actual master Tailscale IP
REDIS_PORT         = 6379
PUBLISH_INTERVAL   = 1                # seconds between heartbeats
KEY_TTL            = 5                # Redis key expires if agent dies

HOT_THRESHOLD      = 75.0             # °C — triggers migration
WARM_THRESHOLD     = 65.0             # °C — warning zone
REENTRY_THRESHOLD  = 70.0             # °C — node re-joins after cooling

# ── CONNECT ──────────────────────────────────────────────────────────────────
r    = redis.Redis(host=MASTER_IP, port=REDIS_PORT, decode_responses=True)
node = socket.gethostname()

print(f"[thermal_agent] Starting on node: {node}")
print(f"[thermal_agent] Publishing to Redis at {MASTER_IP}:{REDIS_PORT}")


def get_cpu_temp() -> float:
    """
    Read CPU package temperature.
    Priority order:
      1. coretemp  (Intel — confirmed working on your VivoBook)
      2. k10temp   (AMD)
      3. acpitz    (ACPI fallback — less accurate but present on most laptops)
      4. cpu_percent * 0.9  (last resort estimate — not a real temp)
    """
    sensors = psutil.sensors_temperatures()

    # Try Intel coretemp first — this is what your machine has
    for source in ("coretemp", "k10temp", "acpitz"):
        entries = sensors.get(source, [])
        if entries:
            # Prefer "Package id 0" label, otherwise take first reading
            pkg = next(
                (e.current for e in entries if "package" in e.label.lower()),
                entries[0].current
            )
            return round(pkg, 1)

    # Absolute fallback — not a real temperature, just an estimate
    print(f"[thermal_agent] WARNING: no hardware temp sensor found, using estimate")
    return round(psutil.cpu_percent(interval=0.1) * 0.9, 1)


def compute_scores(temp: float, load: float) -> dict:
    """
    Compute the three scheduling sub-scores.

    thermal_score:
      Uses the stepped classification from the literature (Zhou 2010):
        1.0  if COOL  (< 65°C)   — full weight
        0.5  if WARM  (65–75°C)  — degraded weight
        0.0  if HOT   (≥ 75°C)   — node excluded from dispatch
      This stepped approach is deliberately conservative — a WARM node
      should not be treated as equivalent to a COOL one.

    load_score:
      Linear: 1 - (load / 100). A node at 30% load scores 0.70.
      Normalised to [0.0, 1.0].

    locality_score:
      Fixed at 0.5 (unknown). This is the honest default — we do not
      track where each task's input data lives. Setting it to 0.8 for
      all nodes (as in the previous version) made the formula misleading.
      It remains in the formula so the weights still sum to 1.0, but it
      contributes equally to all nodes and therefore does not bias routing.

    Final score:
      score = 0.5 × thermal + 0.3 × load + 0.2 × locality
      Derived from: Zhou (TACO 2010), Ray (OSDI 2018), Bashir (2018)
    """
    if temp >= HOT_THRESHOLD:
        thermal_score = 0.0
    elif temp >= WARM_THRESHOLD:
        thermal_score = 0.5
    else:
        thermal_score = 1.0

    load_score    = round(max(0.0, 1.0 - (load / 100.0)), 3)
    locality_score = 0.5   # honest default — see docstring above

    final_score = round(
        (0.5 * thermal_score) + (0.3 * load_score) + (0.2 * locality_score),
        3
    )
    return {
        "thermal_score":  thermal_score,
        "load_score":     load_score,
        "locality_score": locality_score,
        "final_score":    final_score,
    }


def classify_status(temp: float) -> str:
    if temp >= HOT_THRESHOLD:
        return "HOT"
    elif temp >= WARM_THRESHOLD:
        return "WARM"
    return "COOL"


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
print(f"[thermal_agent] Running. Press Ctrl+C to stop.\n")

while True:
    try:
        temp   = get_cpu_temp()
        load   = round(psutil.cpu_percent(interval=0.5), 1)
        scores = compute_scores(temp, load)
        status = classify_status(temp)

        data = {
            "node":           node,
            "cpu_temp":       temp,
            "cpu_load":       load,
            "status":         status,
            "thermal_score":  scores["thermal_score"],
            "load_score":     scores["load_score"],
            "locality_score": scores["locality_score"],
            "final_score":    scores["final_score"],
            "timestamp":      round(time.time(), 2),
        }

        key = f"node:status:{node}"
        r.set(key, json.dumps(data))
        r.expire(key, KEY_TTL)

        print(
            f"[{node}] temp={temp}°C  load={load}%  "
            f"status={status}  score={scores['final_score']}"
        )

    except redis.ConnectionError as e:
        print(f"[thermal_agent] Redis connection lost: {e}  — retrying in 3s")
        time.sleep(3)
        continue
    except Exception as e:
        print(f"[thermal_agent] Error: {e}")

    time.sleep(PUBLISH_INTERVAL)
