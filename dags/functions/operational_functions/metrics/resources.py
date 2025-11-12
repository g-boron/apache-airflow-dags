import os
import psutil
import time
import csv
from functools import wraps
import threading


def measure_resources(interval=0.5):
    """
    Decorator responsible for measure Airflow task resources - CPU, RAM and
    task duration. Creates CSV file with name: <dag_id>.csv.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            dag = kwargs.get("dag")
            dag_run = kwargs.get("dag_run")
            dag_id = dag.dag_id if dag else "unknown_dag"
            run_id = dag_run.run_id if dag_run else "manual_run"
            log_dir = "/opt/airflow/logs/metrics"
            os.makedirs(log_dir, exist_ok=True)
            file_path = os.path.join(log_dir, f"{dag_id}.csv")

            pid = os.getpid()
            process = psutil.Process(pid)

            cpu_samples = []
            ram_samples = []
            stop_flag = threading.Event()

            def _monitor():
                """Monitors memory with intervals."""
                while not stop_flag.is_set():
                    cpu_samples.append(process.cpu_percent(interval=None))
                    ram_samples.append(
                        process.memory_info().rss / (1024 * 1024))  # MB
                    time.sleep(interval)

            monitor_thread = threading.Thread(target=_monitor)
            monitor_thread.start()

            start_time = time.time()
            try:
                result = func(*args, **kwargs)
            finally:
                # Stop monitoring
                stop_flag.set()
                monitor_thread.join()
                end_time = time.time()

                # Calculate average and max values
                avg_cpu = sum(cpu_samples) / len(
                    cpu_samples) if cpu_samples else 0
                max_cpu = max(cpu_samples) if cpu_samples else 0
                avg_ram = sum(ram_samples) / len(
                    ram_samples) if ram_samples else 0
                max_ram = max(ram_samples) if ram_samples else 0
                wall_time = end_time - start_time

                row = {
                    "task_name": func.__name__,
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_duration_s": round(wall_time, 2),
                    "avg_cpu_percent": round(avg_cpu, 2),
                    "max_cpu_percent": round(max_cpu, 2),
                    "avg_ram_mb": round(avg_ram, 2),
                    "max_ram_mb": round(max_ram, 2)
                }

                write_header = not os.path.exists(file_path)
                with open(file_path, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=row.keys())
                    if write_header:
                        writer.writeheader()
                    writer.writerow(row)

            return result

        return wrapper

    return decorator
