# tests/test_dags.py
"""
Tests for Airflow DAG structure using AST parsing.
Verifies task IDs, schedules, dependencies, and retry config
without requiring Airflow to be installed.
"""
import ast
from pathlib import Path


def load_dag_source(filename: str) -> str:
    """
    Load DAG source code from the dags/ directory.

    Args:
        filename: DAG filename.

    Returns:
        str: Source code of the DAG file.
    """
    path = Path("dags") / filename
    return path.read_text(encoding="utf-8")


def extract_string_values(source: str, keyword: str) -> list[str]:
    """
    Extract string values assigned to a keyword in source code.

    Args:
        source: Python source code string.
        keyword: Keyword to search for (e.g. 'task_id').

    Returns:
        list of string values found.
    """
    tree = ast.parse(source)
    values = []
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword):
            if node.arg == keyword and isinstance(node.value, ast.Constant):
                values.append(node.value.value)
    return values


def extract_dag_id(source: str) -> str | None:
    """
    Extract the dag_id value from DAG source code.

    Args:
        source: Python source code string.

    Returns:
        str: dag_id value or None.
    """
    values = extract_string_values(source, "dag_id")
    return values[0] if values else None


def extract_schedule(source: str) -> str | None:
    """
    Extract the schedule_interval value from DAG source code.

    Args:
        source: Python source code string.

    Returns:
        str: schedule_interval value or None.
    """
    values = extract_string_values(source, "schedule_interval")
    return values[0] if values else None


def extract_task_ids(source: str) -> list[str]:
    """
    Extract all task_id values from DAG source code.

    Args:
        source: Python source code string.

    Returns:
        list of task_id strings.
    """
    return extract_string_values(source, "task_id")


class TestImageryPipelineDag:
    """Tests for imagery_pipeline_dag.py structure."""

    def setup_method(self):
        self.source = load_dag_source("imagery_pipeline_dag.py")

    def test_dag_file_exists(self):
        """imagery_pipeline_dag.py exists in dags/."""
        assert Path("dags/imagery_pipeline_dag.py").exists()

    def test_dag_id(self):
        """DAG id is imagery_pipeline."""
        assert extract_dag_id(self.source) == "imagery_pipeline"

    def test_schedule_is_weekly(self):
        """DAG schedule is weekly."""
        assert extract_schedule(self.source) == "@weekly"

    def test_has_fetch_task(self):
        """DAG has fetch_sentinel_tile task."""
        assert "fetch_sentinel_tile" in extract_task_ids(self.source)

    def test_has_process_task(self):
        """DAG has process_tile task."""
        assert "process_tile" in extract_task_ids(self.source)

    def test_has_upload_task(self):
        """DAG has upload_tile_to_minio task."""
        assert "upload_tile_to_minio" in extract_task_ids(self.source)

    def test_has_change_detection_task(self):
        """DAG has run_change_detection task."""
        assert "run_change_detection" in extract_task_ids(self.source)

    def test_has_score_anomalies_task(self):
        """DAG has score_anomalies task."""
        assert "score_anomalies" in extract_task_ids(self.source)

    def test_has_five_tasks(self):
        """DAG has exactly five tasks."""
        assert len(extract_task_ids(self.source)) == 5

    def test_has_retries(self):
        """DAG has retries configured."""
        assert "retries" in self.source

    def test_dependency_chain_present(self):
        """DAG has task dependency chain."""
        assert ">>" in self.source


class TestAnomalyLoaderDag:
    """Tests for anomaly_loader_dag.py structure."""

    def setup_method(self):
        self.source = load_dag_source("anomaly_loader_dag.py")

    def test_dag_file_exists(self):
        """anomaly_loader_dag.py exists in dags/."""
        assert Path("dags/anomaly_loader_dag.py").exists()

    def test_dag_id(self):
        """DAG id is anomaly_loader."""
        assert extract_dag_id(self.source) == "anomaly_loader"

    def test_schedule_is_daily(self):
        """DAG schedule is daily."""
        assert extract_schedule(self.source) == "@daily"

    def test_has_loader_task(self):
        """DAG has load_anomalies_to_snowflake task."""
        assert "load_anomalies_to_snowflake" in extract_task_ids(self.source)

    def test_has_retries(self):
        """DAG has retries configured."""
        assert "retries" in self.source

    def test_has_owner(self):
        """DAG has owner configured."""
        assert "geo-pipeline" in self.source
