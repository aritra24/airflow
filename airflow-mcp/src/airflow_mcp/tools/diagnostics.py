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

from fastmcp import Context

from airflow_mcp.app import get_client, mcp


@mcp.tool()
def diagnose_dag_run(ctx: Context, dag_id: str, dag_run_id: str) -> dict:
    """
    Get diagnostic information for a specific Dag run.

    This composite tool fetches the overall run state AND the status of all
    task instances within it in a single call.

    Parameters
    ----------
    dag_id : str
        The ID of the Dag.
    dag_run_id : str
        The ID of the Dag run.

    Returns
    -------
    dict
        A dictionary containing 'dag_run' details and its 'task_instances'.
    """
    with get_client(ctx) as client:
        return client.diagnose_dag_run(dag_id, dag_run_id)
