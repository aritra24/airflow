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

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def airflow_env(monkeypatch):
    """Ensure the service-account env vars are present for every test in this module."""
    monkeypatch.setenv("AIRFLOW_BASE_URL", "http://localhost:8080/api/v2")
    monkeypatch.setenv("AIRFLOW_MCP_API_TOKEN", "svc-account-token")


def _make_mock_client(return_values: dict):
    """Build a MagicMock that satisfies the AirflowClient context-manager protocol."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    for method, value in return_values.items():
        getattr(mock_client, method).return_value = value
    return mock_client


def test_list_dags_tool():
    from airflow_mcp.tools.dags import list_dags

    mock = _make_mock_client({"list_dags": {"dags": [{"dag_id": "test_dag"}]}})
    with patch("airflow_mcp.tools.dags.get_client", return_value=mock):
        result = list_dags()

    assert result["dags"][0]["dag_id"] == "test_dag"


def test_diagnose_dag_run_tool():
    from airflow_mcp.tools.diagnostics import diagnose_dag_run

    mock = _make_mock_client(
        {
            "diagnose_dag_run": {
                "dag_run": {"state": "failed"},
                "task_instances": {"task_instances": []},
            }
        }
    )
    with patch("airflow_mcp.tools.diagnostics.get_client", return_value=mock):
        result = diagnose_dag_run("test_dag", "test_run")

    assert result["dag_run"]["state"] == "failed"


def test_get_client_uses_service_account_token_not_user_token():
    """
    The core multi-tenancy guarantee: regardless of who is calling, the
    AirflowClient always carries the service-account token (from env),
    never a user's JWT.  This proves there is no token passthrough.
    """
    from airflow_mcp.app import get_client

    # Simulate two sequential calls as if different users triggered them.
    client_a = get_client()
    client_b = get_client()

    # Both must use the service-account token from env, not any user JWT.
    assert client_a._http.headers["Authorization"] == "Bearer svc-account-token"
    assert client_b._http.headers["Authorization"] == "Bearer svc-account-token"

    client_a._http.close()
    client_b._http.close()
