import redis
import cloudpickle
import time
import logging

# Logging setup
logging.basicConfig(
    filename="master.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

# Connect to Redis (MASTER IP)
r = redis.Redis(host='localhost', port=6379)

# Task function
def task():
    import psutil
    import time

    return {
        "cpu": psutil.cpu_percent(interval=1),
        "memory": psutil.virtual_memory().percent
    }

# Clear old queues
r.delete("task_queue")
r.delete("result_queue")

# Send tasks
num_tasks = 10

start = time.time()

for i in range(num_tasks):
    logging.info(f"Sending task {i}")
    print(f"[MASTER] Sending task {i}")
    r.rpush("task_queue", cloudpickle.dumps(task))

# Receive results
for _ in range(num_tasks):
    _, result_data = r.blpop("result_queue")
    result = cloudpickle.loads(result_data)

    logging.info(f"Result received: {result}")
    print("[RESULT]", result)

end = time.time()

print(f"TOTAL TIME: {end - start}")
logging.info(f"Total time: {end - start}")
