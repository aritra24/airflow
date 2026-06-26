# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from airflow_mcp.client import AirflowClient


def test_airflow_client_list_dags(respx_mock):
    respx_mock.get("http://localhost:8080/api/v2/dags").respond(
        json={"dags": [{"dag_id": "test_dag", "is_paused": False}], "total_entries": 1}
    )

    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    dags = client.list_dags()
    assert len(dags["dags"]) == 1
    assert dags["dags"][0]["dag_id"] == "test_dag"

    request = respx_mock.calls.last.request
    assert request.headers["Authorization"] == "Bearer fake-token"
    assert "X-Airflow-On-Behalf-Of" not in request.headers


def test_airflow_client_on_behalf_of_header(respx_mock):
    respx_mock.get("http://localhost:8080/api/v2/dags").respond(json={"dags": [], "total_entries": 0})

    client = AirflowClient(
        base_url="http://localhost:8080/api/v2", token="fake-token", user_id="test_user"
    )
    client.list_dags()

    request = respx_mock.calls.last.request
    assert request.headers["Authorization"] == "Bearer fake-token"
    assert request.headers["X-Airflow-On-Behalf-Of"] == "test_user"


def test_airflow_client_diagnose_dag_run(respx_mock):
    respx_mock.get("http://localhost:8080/api/v2/dags/test_dag/dagRuns/test_run").respond(
        json={"dag_id": "test_dag", "dag_run_id": "test_run", "state": "failed"}
    )
    respx_mock.get("http://localhost:8080/api/v2/dags/test_dag/dagRuns/test_run/taskInstances").respond(
        json={"task_instances": [{"task_id": "failed_task", "state": "failed"}], "total_entries": 1}
    )

    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    diagnosis = client.diagnose_dag_run("test_dag", "test_run")

    assert diagnosis["dag_run"]["state"] == "failed"
    assert len(diagnosis["task_instances"]["task_instances"]) == 1
    assert diagnosis["task_instances"]["task_instances"][0]["task_id"] == "failed_task"


def test_airflow_client_get_dag(respx_mock):
    respx_mock.get("http://localhost:8080/api/v2/dags/test_dag").respond(
        json={"dag_id": "test_dag", "description": "Test"}
    )
    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    dag = client.get_dag("test_dag")
    assert dag["dag_id"] == "test_dag"


def test_airflow_client_list_dag_runs(respx_mock):
    respx_mock.get("http://localhost:8080/api/v2/dags/test_dag/dagRuns").respond(
        json={"dag_runs": [{"dag_run_id": "run_1"}], "total_entries": 1}
    )
    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    runs = client.list_dag_runs("test_dag")
    assert runs["dag_runs"][0]["dag_run_id"] == "run_1"


def test_airflow_client_list_task_instances(respx_mock):
    respx_mock.get("http://localhost:8080/api/v2/dags/test_dag/dagRuns/test_run/taskInstances").respond(
        json={"task_instances": [{"task_id": "task_1"}], "total_entries": 1}
    )
    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    tis = client.list_task_instances("test_dag", "test_run")
    assert tis["task_instances"][0]["task_id"] == "task_1"


def test_airflow_client_get_task_instance(respx_mock):
    respx_mock.get(
        "http://localhost:8080/api/v2/dags/test_dag/dagRuns/test_run/taskInstances/task_1"
    ).respond(json={"task_id": "task_1", "state": "success"})
    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    ti = client.get_task_instance("test_dag", "test_run", "task_1")
    assert ti["state"] == "success"


def test_airflow_client_get_task_logs(respx_mock):
    respx_mock.get(
        "http://localhost:8080/api/v2/dags/test_dag/dagRuns/test_run/taskInstances/task_1/logs/1"
    ).respond(json={"content": "log data", "continuation_token": None})
    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    logs = client.get_task_logs("test_dag", "test_run", "task_1", 1)
    assert logs["content"] == "log data"


def test_airflow_client_list_variables(respx_mock):
    respx_mock.get("http://localhost:8080/api/v2/variables").respond(
        json={"variables": [{"key": "foo", "value": "bar"}], "total_entries": 1}
    )
    client = AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token")
    variables = client.list_variables()
    assert variables["variables"][0]["key"] == "foo"


def test_airflow_client_context_manager_closes_connection(respx_mock):
    """AirflowClient used as a context manager must close its httpx.Client on __exit__."""
    respx_mock.get("http://localhost:8080/api/v2/dags").respond(json={"dags": [], "total_entries": 0})

    with AirflowClient(base_url="http://localhost:8080/api/v2", token="fake-token") as client:
        result = client.list_dags()

    assert result["total_entries"] == 0
    assert client._http.is_closed
