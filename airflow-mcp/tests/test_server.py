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
    from fastmcp import Context

    mock_ctx = MagicMock(spec=Context)
    with patch("airflow_mcp.tools.dags.get_client"), patch("airflow_mcp.tools.dags.DAGApi") as mock_dag_api:
        mock_dag_api.return_value.get_dags.return_value.to_dict.return_value = {"dags": [{"dag_id": "test_dag"}]}
        result = list_dags(mock_ctx)

    assert result["dags"][0]["dag_id"] == "test_dag"


def test_diagnose_dag_run_tool():
    from airflow_mcp.tools.diagnostics import diagnose_dag_run
    from fastmcp import Context

    mock_ctx = MagicMock(spec=Context)
    
    with patch("airflow_mcp.tools.diagnostics.get_client"), \
         patch("airflow_mcp.tools.diagnostics.DagRunApi") as mock_dag_run_api, \
         patch("airflow_mcp.tools.diagnostics.TaskInstanceApi") as mock_task_instance_api:
        
        mock_dag_run_api.return_value.get_dag_run.return_value.to_dict.return_value = {"state": "failed"}
        mock_task_instance_api.return_value.get_task_instances.return_value.to_dict.return_value = {"task_instances": []}
        
        result = diagnose_dag_run(mock_ctx, "test_dag", "test_run")

    assert result["dag_run"]["state"] == "failed"


def test_get_client_uses_service_account_token_not_user_token():
    """
    The core multi-tenancy guarantee: regardless of who is calling, the
    AirflowClient always carries the service-account token (from env),
    never a user's JWT.  This proves there is no token passthrough.
    """
    from airflow_mcp.app import get_client
    from fastmcp import Context

    # Simulate two sequential calls as if different users triggered them.
    mock_ctx_a = MagicMock(spec=Context)
    mock_ctx_a.request_context.auth.claims = {"sub": "alice"}
    
    mock_ctx_b = MagicMock(spec=Context)
    mock_ctx_b.request_context.auth.claims = {"sub": "bob"}
    
    client_a = get_client(mock_ctx_a)
    client_b = get_client(mock_ctx_b)

    # Both must use the service-account token from env, not any user JWT.
    assert client_a.configuration.access_token == "svc-account-token"
    assert client_b.configuration.access_token == "svc-account-token"
    
    # But they must carry the impersonation header.
    assert client_a.default_headers["X-Airflow-On-Behalf-Of"] == "alice"
    assert client_b.default_headers["X-Airflow-On-Behalf-Of"] == "bob"

