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

from typing import Any

import httpx


class AirflowClient:
    """
    Thin HTTP client for the Airflow REST API v2.

    Must be used as a context manager so the underlying httpx connection pool
    is released after each tool call:

        with AirflowClient(...) as client:
            return client.list_dags()
    """

    DEFAULT_TIMEOUT = httpx.Timeout(timeout=30.0, connect=10.0)

    def __init__(self, base_url: str, token: str) -> None:
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {token}"},
            timeout=self.DEFAULT_TIMEOUT,
        )

    def __enter__(self) -> AirflowClient:
        return self

    def __exit__(self, *_: object) -> None:
        self._http.close()

    # ------------------------------------------------------------------ #
    # Dag endpoints                                                        #
    # ------------------------------------------------------------------ #

    def list_dags(self) -> dict[str, Any]:
        """List all Dags."""
        response = self._http.get("/dags")
        response.raise_for_status()
        return response.json()

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        """Get basic information about a Dag."""
        response = self._http.get(f"/dags/{dag_id}")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------ #
    # Dag Run endpoints                                                    #
    # ------------------------------------------------------------------ #

    def list_dag_runs(self, dag_id: str, limit: int = 100) -> dict[str, Any]:
        """List Dag runs for a specific Dag."""
        response = self._http.get(f"/dags/{dag_id}/dagRuns", params={"limit": limit})
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------ #
    # Task Instance endpoints                                              #
    # ------------------------------------------------------------------ #

    def list_task_instances(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """List task instances for a specific Dag run."""
        response = self._http.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
        response.raise_for_status()
        return response.json()

    def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str) -> dict[str, Any]:
        """Get details of a specific task instance."""
        response = self._http.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}")
        response.raise_for_status()
        return response.json()

    def get_task_logs(
        self, dag_id: str, dag_run_id: str, task_id: str, task_try_number: int
    ) -> dict[str, Any]:
        """Get logs for a specific task instance try."""
        response = self._http.get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"
        )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------ #
    # Composite / diagnostic                                               #
    # ------------------------------------------------------------------ #

    def diagnose_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """Fetch the Dag run state and all task instance states in one call."""
        run_resp = self._http.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        run_resp.raise_for_status()

        ti_resp = self._http.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
        ti_resp.raise_for_status()

        return {
            "dag_run": run_resp.json(),
            "task_instances": ti_resp.json(),
        }

    # ------------------------------------------------------------------ #
    # Variables                                                            #
    # ------------------------------------------------------------------ #

    def list_variables(self) -> dict[str, Any]:
        """
        List Airflow variables.

        Note: variable *values* are returned by the Airflow API and may contain
        secrets.  In a multi-tenant deployment, access to this method should be
        gated by an admin scope on the caller's JWT.
        """
        response = self._http.get("/variables")
        response.raise_for_status()
        return response.json()
