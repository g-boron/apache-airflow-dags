""" UNIT TEST """
from functions.operational_functions.metrics.resources import get_dag_info


class TestDAG:
  dag_id: str = "TEST_DAG"


class TestRun:
  run_id: str = "TEST_RUN"


TEST_KWARGS = {
  "dag": TestDAG,
  "dag_run": TestRun
}

TEST_KWARGS_MISSING = {
  "dag": None,
  "dag_run": None
}


def test_get_dag_info() -> None:
  dag_id, run_id = get_dag_info(**TEST_KWARGS)

  assert dag_id == "TEST_DAG"
  assert run_id == "TEST_RUN"


def test_missing_keys() -> None:
  dag_id, run_id = get_dag_info(**TEST_KWARGS_MISSING)

  assert dag_id == "unknown_dag"
  assert run_id == "manual_run"
