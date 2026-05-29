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

from airflow_mcp.app import get_client, mcp


@mcp.tool()
def list_dags() -> dict:
    """
    List all available Dags in the Airflow environment.

    Returns
    -------
    dict
        A dictionary containing a list of dags and the total count.
    """
    with get_client() as client:
        return client.list_dags()


@mcp.tool()
def get_dag(dag_id: str) -> dict:
    """
    Get basic information about a specific Dag.

    Parameters
    ----------
    dag_id : str
        The ID of the Dag.

    Returns
    -------
    dict
        A dictionary containing the Dag's details.
    """
    with get_client() as client:
        return client.get_dag(dag_id)


@mcp.tool()
def list_dag_runs(dag_id: str, limit: int = 100) -> dict:
    """
    List historical executions (Dag Runs) for a specific Dag.

    Parameters
    ----------
    dag_id : str
        The ID of the Dag.
    limit : int
        Maximum number of runs to return (default is 100).

    Returns
    -------
    dict
        A dictionary containing the list of dag runs.
    """
    with get_client() as client:
        return client.list_dag_runs(dag_id, limit)
