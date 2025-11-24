""" Measuring DAGs performance. """
import os
import psutil
import time
import csv
from functools import wraps
import threading
from typing import Any


def get_dag_info(**kwargs) -> tuple[str, str]:
  """
  Gets DAG information (dag_id and run_id) from context.

  :param kwargs: Config dictionary.
  :return: DAG ID and run ID.
  """
  dag = kwargs.get("dag")
  dag_run = kwargs.get("dag_run")
  dag_id = dag.dag_id if dag else "unknown_dag"
  run_id = dag_run.run_id if dag_run else "manual_run"
  return dag_id, run_id


def save_results(row: dict, file_path: str) -> None:
  """
  Saves results into given path.

  :param row: Dictionary of calculated metrics.
  :param file_path: File path to save.
  """
  write_header = not os.path.exists(file_path)
  with open(file_path, "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=row.keys())
    if write_header:
      writer.writeheader()
    writer.writerow(row)


def calculate_metrics_and_save(
  cpu_samples: list, ram_samples: list, end_time: time, start_time: time, 
  func: callable, dag_id: str, run_id: str, file_path: str
) -> None:
  """
  Calculates metrics and saves results into file.
  Calculates: average CPU usage, maximum CPU usage, average RAM usage,
  maximum RAM usage, task duration.

  :param cpu_samples: CPU usage data.
  :param ram_samples: RAM usage data.
  :param end_time: Function end time.
  :param start_time: Function start time.
  :param func: Function reference.
  :param dag_id: DAG ID
  :param run_id: DAG run ID
  :param file_path: Path to save.
  """
  avg_cpu = sum(cpu_samples) / len(
    cpu_samples) if cpu_samples else 0
  max_cpu = max(cpu_samples, default=0)
  avg_ram = sum(ram_samples) / len(
    ram_samples) if ram_samples else 0
  max_ram = max(ram_samples, default=0)
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
  save_results(row, file_path)


def measure_resources(interval: float = 0.5) -> callable:
  """
  Decorator responsible for measure Airflow task resources - CPU, RAM and
  task duration. Creates CSV file with name: <dag_id>.csv.

  :param interval: Storing usage data interval.
  """
  def decorator(func: callable) -> callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
      dag_id, run_id = get_dag_info(**kwargs)
      log_dir = "/opt/airflow/logs/metrics"
      os.makedirs(log_dir, exist_ok=True)
      file_path = os.path.join(log_dir, f"{dag_id}.csv")

      pid = os.getpid()
      process = psutil.Process(pid)

      cpu_samples = []
      ram_samples = []
      stop_flag = threading.Event()

      def _monitor() -> None:
        """
        Monitors memory with intervals.
        """
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

        calculate_metrics_and_save(
          cpu_samples, ram_samples, end_time, start_time, func, dag_id, run_id,
          file_path
        )

      return result

    return wrapper

  return decorator
