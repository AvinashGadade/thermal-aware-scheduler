"""
yolo_workload.py
----------------
Run on MASTER NODE ONLY.

Wraps the YOLO inference workload into individual task functions
that can be submitted to migration_engine.submit_task().

HOW IT WORKS
------------
1. Scans IMAGE_DIR for all jpg/png files
2. Splits them into chunks (one chunk = one task)
3. Each task function:
     - loads the YOLO model fresh (so it works after cloudpickle serialisation)
     - builds variants of each image (resize, blur, grayscale, edge, flip)
     - runs YOLO inference REPEAT_PER_IMAGE times on each variant
     - returns a result dict  {node, chunk_id, images_processed, time_taken}
4. Submits all chunk tasks to migration_engine
5. Collects results from result_queue in Redis
6. Prints a final summary

WHY CHUNKS INSTEAD OF ONE TASK PER IMAGE
-----------------------------------------
One task per image = thousands of Redis round-trips + cloudpickle overhead per image.
Chunks of 10-20 images = manageable queue depth, meaningful CPU load per task,
and the task runs long enough for the thermal agent to detect the temperature rise.

USAGE
-----
  # Terminal 1 (master):   python thermal_agent.py
  # Terminal 2 (master):   python global_scheduler.py
  # Terminal 3 (client):   python thermal_agent.py
  # Terminal 4 (client):   python local_scheduler.py
  # Terminal 5 (master):   python yolo_workload.py        ← this file
"""

import os
import time
import math
import socket
import redis
import cloudpickle

import migration_engine as me

# ── CONFIG ────────────────────────────────────────────────────────────────────
MASTER_IP        = "100.89.81.24"   # your master Tailscale IP
REDIS_PORT       = 6379
IMAGE_DIR        = "val2017"        # folder of jpg/png images on master
MODEL_NAME       = "yolov8m.pt"
IMG_SIZE         = 1280
REPEAT_PER_IMAGE = 4
VARIANTS_PER_IMAGE = 5
CHUNK_SIZE       = 5               # images per task — tune this
LIMIT            = None             # None = all images; set e.g. 50 for quick test

# ── REDIS ─────────────────────────────────────────────────────────────────────
r    = redis.Redis(host=MASTER_IP, port=REDIS_PORT, decode_responses=False)
node = socket.gethostname()


# ── TASK FUNCTION (must be importable by cloudpickle on remote nodes) ─────────

def make_yolo_chunk_task(chunk_id: int, image_paths: list, model_name: str,
                          img_size: int, repeat: int, variants_count: int):
    """
    Returns a zero-argument callable that runs YOLO inference on a list
    of image paths. This closure is what migration_engine.submit_task() receives.

    Returning a closure (not running it here) means:
      - If master stays COOL  → closure runs locally on master
      - If master goes HOT    → closure is cloudpickled and sent to client
    Either way the exact same function runs.
    """

    def task():
        import os
        import time
        import socket
        import cv2
        import psutil
        from ultralytics import YOLO

        executing_node = socket.gethostname()
        model = YOLO(model_name)
        import numpy as np

        def cpu_burn(seconds=2.0):
            """Pure CPU matrix multiply to heat up faster."""
            end = time.time() + seconds
            while time.time() < end:
                a = np.random.rand(512, 512)
                b = np.random.rand(512, 512)
                _ = np.dot(a, b)

        # Call it once before inference loop begins:
        cpu_burn(seconds=2.0)

        model.to("cpu")

        def get_cpu_temp():
            try:
                temps = psutil.sensors_temperatures()
                for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
                    if key in temps and temps[key]:
                        vals = [x.current for x in temps[key] if x.current is not None]
                        if vals:
                            return round(max(vals), 1)
            except Exception:
                pass
            return -1.0

        def build_variants(img):
            base  = cv2.resize(img, (img_size, img_size), interpolation=cv2.INTER_LINEAR)
            gray  = cv2.cvtColor(base, cv2.COLOR_BGR2GRAY)
            gray3 = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
            blur  = cv2.GaussianBlur(base, (7, 7), 1.5)
            edges = cv2.Canny(gray, 80, 180)
            edges3 = cv2.cvtColor(edges, cv2.COLOR_GRAY2BGR)
            flip  = cv2.flip(base, 1)
            all_v = [base, blur, gray3, edges3, flip]
            return all_v[:variants_count]

        processed = 0
        errors    = 0
        t_start   = time.time()
        temp_start = get_cpu_temp()

        for img_path in image_paths:
            try:
                img = cv2.imread(img_path)
                if img is None:
                    errors += 1
                    continue

                variants = build_variants(img)

                for _ in range(repeat):
                    for v in variants:
                        model.predict(
                            source=v,
                            imgsz=img_size,
                            device="cpu",
                            verbose=False
                        )
                processed += 1

            except Exception as e:
                errors += 1
                print(f"[yolo_task chunk={chunk_id}] Error on {img_path}: {e}")

        t_end     = time.time()
        temp_end  = get_cpu_temp()
        elapsed   = round(t_end - t_start, 2)

        result = {
            "chunk_id":         chunk_id,
            "node":             executing_node,
            "images_processed": processed,
            "errors":           errors,
            "time_sec":         elapsed,
            "temp_start_c":     temp_start,
            "temp_end_c":       temp_end,
            "throughput":       round(processed / elapsed, 3) if elapsed > 0 else 0,
        }

        print(
            f"[yolo_task] chunk={chunk_id} | node={executing_node} | "
            f"done={processed} | errs={errors} | "
            f"time={elapsed}s | temp {temp_start}→{temp_end}°C"
        )
        return result

    return task


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    # ── 1. Discover images ───────────────────────────────────────────────────
    if not os.path.isdir(IMAGE_DIR):
        print(f"[yolo_workload] ERROR: IMAGE_DIR '{IMAGE_DIR}' not found.")
        print(f"[yolo_workload] Download COCO val2017 or point IMAGE_DIR at your image folder.")
        return

    all_images = sorted([
        os.path.join(IMAGE_DIR, f)
        for f in os.listdir(IMAGE_DIR)
        if f.lower().endswith((".jpg", ".jpeg", ".png"))
    ])

    if LIMIT is not None:
        all_images = all_images[:LIMIT]

    total_images = len(all_images)

    if total_images == 0:
        print(f"[yolo_workload] ERROR: No images found in '{IMAGE_DIR}'")
        return

    # ── 2. Split into chunks ─────────────────────────────────────────────────
    chunks = [
        all_images[i : i + CHUNK_SIZE]
        for i in range(0, total_images, CHUNK_SIZE)
    ]
    total_chunks = len(chunks)

    print("\n" + "=" * 64)
    print("  THERMAL-AWARE YOLO WORKLOAD")
    print("=" * 64)
    print(f"  Node:           {node}")
    print(f"  Image dir:      {IMAGE_DIR}")
    print(f"  Total images:   {total_images}")
    print(f"  Chunk size:     {CHUNK_SIZE} images/task")
    print(f"  Total tasks:    {total_chunks}")
    print(f"  Model:          {MODEL_NAME}")
    print(f"  Image size:     {IMG_SIZE}")
    print(f"  Repeat/image:   {REPEAT_PER_IMAGE}")
    print(f"  Variants/image: {VARIANTS_PER_IMAGE}")
    print(f"  HOT threshold:  {me.HOT_THRESHOLD}°C  (migration fires here)")
    print("=" * 64 + "\n")

    # ── 3. Start migration engine ────────────────────────────────────────────
    me.start()

    print("[yolo_workload] Waiting 3s for thermal agents to publish readings...\n")
    time.sleep(3)

    # ── 4. Submit all chunk tasks ────────────────────────────────────────────
    print(f"[yolo_workload] Submitting {total_chunks} chunk tasks...\n")

    submit_start = time.time()

    for chunk_id, chunk_paths in enumerate(chunks):
        task_fn = make_yolo_chunk_task(
            chunk_id      = chunk_id,
            image_paths   = chunk_paths,
            model_name    = MODEL_NAME,
            img_size      = IMG_SIZE,
            repeat        = REPEAT_PER_IMAGE,
            variants_count= VARIANTS_PER_IMAGE,
        )
        me.submit_task(task_fn)
        print(f"[yolo_workload] Submitted chunk {chunk_id + 1}/{total_chunks}  ({len(chunk_paths)} images)")

    print(f"\n[yolo_workload] All {total_chunks} tasks submitted. Collecting results...\n")

    # ── 5. Collect results ───────────────────────────────────────────────────
    results       = []
    local_count   = 0
    remote_count  = 0
    error_count   = 0

    collect_start = time.time()

    for i in range(total_chunks):
        try:
            _, raw = r.blpop("result_queue", timeout=600)
            result_outer = cloudpickle.loads(raw)

            status   = result_outer.get("status", "?")
            hostname = result_outer.get("hostname", "?")
            mode     = result_outer.get("mode", "?")
            inner    = result_outer.get("result", {})

            if status == "error":
                error_count += 1
                print(f"  [{i+1}/{total_chunks}] ERROR from {hostname}: {result_outer.get('error','?')}")
                continue

            chunk_id   = inner.get("chunk_id", "?")
            done       = inner.get("images_processed", 0)
            t          = inner.get("time_sec", 0)
            temp_s     = inner.get("temp_start_c", "?")
            temp_e     = inner.get("temp_end_c", "?")
            throughput = inner.get("throughput", 0)

            results.append(inner)

            if mode == "local":
                local_count += 1
            else:
                remote_count += 1

            print(
                f"  [{i+1}/{total_chunks}] chunk={chunk_id} | "
                f"node={hostname} | mode={mode} | "
                f"images={done} | time={t}s | "
                f"temp {temp_s}→{temp_e}°C | {throughput} img/s"
            )

        except Exception as e:
            error_count += 1
            print(f"  [{i+1}/{total_chunks}] Result decode error: {e}")

    # ── 6. Final summary ─────────────────────────────────────────────────────
    total_elapsed   = round(time.time() - collect_start, 2)
    total_processed = sum(r.get("images_processed", 0) for r in results)
    avg_throughput  = round(total_processed / total_elapsed, 3) if total_elapsed > 0 else 0

    print("\n" + "=" * 64)
    print("  FINAL SUMMARY")
    print("=" * 64)
    print(f"  Total chunks submitted:  {total_chunks}")
    print(f"  Ran locally on master:   {local_count}")
    print(f"  Migrated to client:      {remote_count}")
    print(f"  Errors:                  {error_count}")
    print(f"  Total images processed:  {total_processed}")
    print(f"  Total wall time:         {total_elapsed}s")
    print(f"  Overall throughput:      {avg_throughput} images/sec")
    print("=" * 64 + "\n")

    me.print_stats()


if __name__ == "__main__":
    main()
