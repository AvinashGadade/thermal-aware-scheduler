"""
global_scheduler.py
Run on MASTER NODE ONLY — start it FIRST and leave it running forever.
It watches task_queue:pending 24/7. Tasks land there from two sources:
  1. migration_engine drains master queue when master goes HOT
  2. local_scheduler drains client queue when client goes HOT  ← NEW
Either way this scheduler re-dispatches to whoever scores best RIGHT NOW.
"""
import redis, json, time, cloudpickle, socket

MASTER_IP   = "100.89.81.24"
REDIS_PORT  = 6379
PENDING_KEY = "task_queue:pending"

r    = redis.Redis(host=MASTER_IP, port=REDIS_PORT, decode_responses=False)
node = socket.gethostname()

print(f"[global_scheduler] Started on {node}")
print(f"[global_scheduler] Watching: {PENDING_KEY}  (runs 24/7 — not only when master is HOT)")
print(f"[global_scheduler] Nodes discovered dynamically from Redis\n")


def get_all_nodes() -> list:
    nodes = []
    for key in r.keys(b"node:status:*"):
        raw = r.get(key)
        if raw:
            try:
                nodes.append(json.loads(raw))
            except Exception:
                pass
    return nodes


def choose_best_node(nodes: list) -> str | None:
    if not nodes:
        return None
    available = [n for n in nodes if n.get("status") != "HOT"]
    if not available:
        print("[global_scheduler] All nodes HOT — routing to least-bad score")
        available = nodes
    best = max(available, key=lambda n: n.get("final_score", 0))
    return best["node"]


def dispatch_one(task_bytes: bytes) -> bool:
    nodes = get_all_nodes()
    if not nodes:
        print("[global_scheduler] No nodes online. Retrying...")
        time.sleep(2)
        return False

    target = choose_best_node(nodes)
    if target is None:
        return False

    # Works for master OR client — just pushes to their named queue
    queue_key = f"task_queue:{target}".encode()
    r.rpush(queue_key, task_bytes)

    best_data = next((n for n in nodes if n["node"] == target), {})
    print(
        f"[global_scheduler] Dispatched -> {target}  "
        f"score={best_data.get('final_score','?')}  "
        f"temp={best_data.get('cpu_temp','?')}°C  "
        f"status={best_data.get('status','?')}"
    )

    # Log for evaluation
    r.rpush(b"dispatch_log", json.dumps({
        "target":    target,
        "score":     best_data.get("final_score"),
        "temp":      best_data.get("cpu_temp"),
        "status":    best_data.get("status"),
        "timestamp": round(time.time(), 2),
        "all_nodes": [{"node": n["node"], "score": n.get("final_score"),
                       "status": n.get("status")} for n in nodes],
    }).encode())

    return True


def show_cluster():
    nodes = get_all_nodes()
    print("[global_scheduler] ── Cluster state ──────────────────────────")
    if not nodes:
        print("  (no nodes — is thermal_agent.py running on all machines?)")
    for n in sorted(nodes, key=lambda x: x.get("node", "")):
        print(
            f"  {n['node']:<38}  "
            f"temp={n.get('cpu_temp','?'):>5}°C  "
            f"load={n.get('cpu_load','?'):>5}%  "
            f"status={n.get('status','?'):<5}  "
            f"score={n.get('final_score','?')}"
        )
    print("──────────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    time.sleep(2)
    show_cluster()
    print(f"[global_scheduler] Listening on '{PENDING_KEY}' ...\n")

    while True:
        try:
            result = r.blpop(PENDING_KEY, timeout=3)
            if result is None:
                continue

            _, task_bytes = result
            while not dispatch_one(task_bytes):
                print("[global_scheduler] Waiting for a node to become available...")
                time.sleep(1)

        except redis.ConnectionError as e:
            print(f"[global_scheduler] Redis error: {e} — retrying in 3s")
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n[global_scheduler] Shutting down.")
            break
        except Exception as e:
            print(f"[global_scheduler] Error: {e}")
            time.sleep(1)
