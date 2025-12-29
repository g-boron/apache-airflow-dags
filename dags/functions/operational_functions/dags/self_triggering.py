""" DAG self-triggering module. """
import logging

from airflow.models import Variable

logger = logging.getLogger("airflow.task")


def init_counter(dag_id: str) -> None:  # pragma: no cover
  """
  Checks DAG counter variable. Creates variable with value 1 if it does not
  exist.

  :param dag_id: DAG name.
  """
  counter_var = f"self_trigger_counter_for_{dag_id}"
  if not Variable.get(counter_var, default_var=None):
    logger.info("Initialized variable.")
    Variable.set(counter_var, 1)


def increment_and_check(dag_id: str, max_runs: int) -> str:  # pragma: no cover
  """
  Increments the self-trigger counter if it less than max runs. Returns the name
  of the trigger task to execute next, otherwise returns the stop trigger task.

  :param dag_id: DAG name.
  :param max_runs: Max runs value set in dags_parameters variable.
  :return: Task name to trigger.
  """
  counter_var = f"self_trigger_counter_for_{dag_id}"
  count = int(Variable.get(counter_var))
  logger.info(f"Actual counter value: {count}, max run value: {max_runs}")
  if count < max_runs:
    Variable.set(counter_var, count + 1)
    return "Check_trigger.trigger_dag"
  return "Check_trigger.stop_dag"


def stop_trigger(dag_id: str) -> None:  # pragma: no cover
  """
  Deletes self-trigger counter variable.

  :param dag_id: DAG name.
  """
  logger.info("Deleting counter variable.")
  Variable.delete(f"self_trigger_counter_for_{dag_id}")
