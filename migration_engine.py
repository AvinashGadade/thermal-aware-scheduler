"""
migration_engine.py
-------------------
Run on MASTER NODE ONLY.

Core behaviour (unchanged):
  1. All submitted tasks go into local queue.Queue first
  2. Thread pool executes them on master's own CPU
  3. Thermal watcher polls master temperature every 1 second
  4. On HOT (>= 75°C):
       - Freezes local queue
       - Pickles all pending tasks with cloudpickle
       - Pushes to task_queue:pending in Redis
       - Enters OFFLOAD mode
  5. On cool-down (<= 70°C):
       - Returns to LOCAL mode

Compatible with yolo_workload.py — the result dict written to result_queue
includes {status, hostname, mode, result} so yolo_workload.py can parse it.
"""

import queue
import threading
import time
import socket
import json
import redis
import cloudpickle
import psutil

# ── CONFIG ────────────────────────────────────────────────────────────────────
MASTER_IP         = "100.89.81.24"   # your master Tailscale IP
REDIS_PORT        = 6379
HOT_THRESHOLD     = 75.0
REENTRY_THRESHOLD = 70.0
MAX_LOCAL_WORKERS = 2                # parallel local executor threads

# ── CONNECT ───────────────────────────────────────────────────────────────────
r           = redis.Redis(host=MASTER_IP, port=REDIS_PORT, decode_responses=False)
node        = socket.gethostname()
local_queue = queue.Queue()

mode      = "LOCAL"
mode_lock = threading.Lock()

stats = {
    "tasks_submitted":  0,
    "tasks_local":      0,
    "tasks_migrated":   0,
    "migration_events": 0,
}


# ── HELPERS ───────────────────────────────────────────────────────────────────

def get_master_temp() -> float:
    """Read master temperature from Redis (published by thermal_agent).
    Falls back to direct psutil read if Redis key not yet populated."""
    raw = r.get(f"node:status:{node}")
    if raw:
        return json.loads(raw)["cpu_temp"]
    sensors = psutil.sensors_temperatures()
    for source in ("coretemp", "k10temp", "acpitz"):
        entries = sensors.get(source, [])
        if entries:
            pkg = next(
                (e.current for e in entries if "package" in e.label.lower()),
                entries[0].current
            )
            return round(pkg, 1)
    return 50.0


def get_master_status() -> str:
    raw = r.get(f"node:status:{node}")
    if raw:
        return json.loads(raw)["status"]
    return "COOL"


def log_migration(n_tasks: int, temp: float):
    event = {
        "from_node":    node,
        "n_tasks":      n_tasks,
        "trigger_temp": temp,
        "timestamp":    round(time.time(), 2),
        "reason":       "HOT",
    }
    r.rpush("migration_log", json.dumps(event))
    print(f"\n[migration_engine] *** MIGRATION EVENT ***")
    print(f"[migration_engine]   Moved {n_tasks} pending task(s) to global scheduler")
    print(f"[migration_engine]   Trigger temp: {temp}°C\n")


# ── THERMAL WATCHER ───────────────────────────────────────────────────────────

def thermal_watcher():
    global mode
    print(f"[thermal_watcher] Started. Polling every 1s.")

    while True:
        try:
            temp   = get_master_temp()
            status = get_master_status()

            with mode_lock:
                current_mode = mode

            # In thermal_watcher(), replace the HOT drain block with:

            if status == "HOT" and current_mode == "LOCAL":
                with mode_lock:
                    mode = "OFFLOAD"

                pending = []
                # Drain in-memory local_queue
                while True:
                    try:
                        pending.append(local_queue.get_nowait())
                    except queue.Empty:
                        break

                # Also drain Redis master queue (tasks that global_scheduler sent back to master)
                MY_REDIS_QUEUE = f"task_queue:{node}".encode()
                while True:
                    raw = r.lpop(MY_REDIS_QUEUE)
                    if not raw:
                        break
                    try:
                        pending.append(cloudpickle.loads(raw))
                    except Exception:
                        r.rpush(b"task_queue:pending", raw)  # push raw if can't deserialize

                if pending:
                    for task_fn in pending:
                        r.rpush("task_queue:pending", cloudpickle.dumps(task_fn))
                        stats["tasks_migrated"] += 1
                    stats["migration_events"] += 1
                    log_migration(len(pending), temp)
                else:
                    print(f"[thermal_watcher] HOT at {temp}°C — queues empty, switching to OFFLOAD")

            elif status != "HOT" and current_mode == "OFFLOAD":
                if temp <= REENTRY_THRESHOLD:
                    with mode_lock:
                        mode = "LOCAL"
                    print(
                        f"[thermal_watcher] Temp dropped to {temp}°C — "
                        f"master returning to LOCAL mode"
                    )

        except Exception as e:
            print(f"[thermal_watcher] Error: {e}")

        time.sleep(1)


# ── LOCAL EXECUTOR ────────────────────────────────────────────────────────────

# Replace your local_executor function with this version:

def local_executor(worker_id: int):
    print(f"[local_executor-{worker_id}] Started")
    MY_REDIS_QUEUE = f"task_queue:{node}".encode()

    while True:
        try:
            with mode_lock:
                current_mode = mode

            if current_mode == "OFFLOAD":
                time.sleep(0.2)
                continue

            # Try in-memory queue first (direct submissions)
            task_fn = None
            try:
                task_fn = local_queue.get_nowait()
                source = "local_queue"
            except queue.Empty:
                # Then check Redis queue (tasks dispatched back by global_scheduler)
                raw = r.lpop(MY_REDIS_QUEUE)
                if raw:
                    task_fn = cloudpickle.loads(raw)
                    source = "redis_queue"
                else:
                    time.sleep(0.1)
                    continue

            # Execute
            try:
                print(f"[local_executor-{worker_id}] Running task from {source} on {node}...")
                t0     = time.time()
                result = task_fn()
                elapsed = round(time.time() - t0, 2)
                stats["tasks_local"] += 1

                r.rpush("result_queue", cloudpickle.dumps({
                    "status":   "success",
                    "hostname": node,
                    "mode":     "local",
                    "result":   result,
                }))
                print(f"[local_executor-{worker_id}] Done in {elapsed}s: {result}")

            except Exception as e:
                print(f"[local_executor-{worker_id}] Task failed: {e}")
                r.rpush("result_queue", cloudpickle.dumps({
                    "status":   "error",
                    "hostname": node,
                    "mode":     "local",
                    "error":    str(e),
                }))

        except Exception as e:
            print(f"[local_executor-{worker_id}] Unexpected error: {e}")
            time.sleep(0.5)


# ── PUBLIC API ────────────────────────────────────────────────────────────────

def submit_task(task_fn):
    """
    Submit one task to the engine.
    LOCAL mode  → goes into local queue (runs on master)
    OFFLOAD mode → goes directly to Redis pending queue (global scheduler routes it)
    """
    stats["tasks_submitted"] += 1

    with mode_lock:
        current_mode = mode

    if current_mode == "LOCAL":
        local_queue.put(task_fn)
        print(
            f"[migration_engine] Task queued locally "
            f"(mode=LOCAL, queue_size={local_queue.qsize()})"
        )
    else:
        r.rpush("task_queue:pending", cloudpickle.dumps(task_fn))
        stats["tasks_migrated"] += 1
        print("[migration_engine] Task sent directly to global scheduler (mode=OFFLOAD)")


def start():
    """Start the migration engine background threads. Call once before submitting tasks."""
    print(f"\n[migration_engine] Starting on master node: {node}")
    print(f"[migration_engine] HOT threshold:      {HOT_THRESHOLD}°C")
    print(f"[migration_engine] Re-entry threshold:  {REENTRY_THRESHOLD}°C")
    print(f"[migration_engine] Local workers:       {MAX_LOCAL_WORKERS}\n")

    t = threading.Thread(target=thermal_watcher, daemon=True)
    t.start()

    for i in range(MAX_LOCAL_WORKERS):
        threading.Thread(target=local_executor, args=(i,), daemon=True).start()

    print("[migration_engine] All threads started. Ready to accept tasks.\n")


def print_stats():
    print("\n[migration_engine] ── STATS ──────────────────────────────────")
    print(f"  Tasks submitted:   {stats['tasks_submitted']}")
    print(f"  Ran locally:       {stats['tasks_local']}")
    print(f"  Migrated:          {stats['tasks_migrated']}")
    print(f"  Migration events:  {stats['migration_events']}")
    print("────────────────────────────────────────────────────────────\n")
