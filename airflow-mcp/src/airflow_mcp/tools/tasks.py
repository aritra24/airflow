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
def list_task_instances(dag_id: str, dag_run_id: str) -> dict:
    """
    List the states of all task instances within a specific Dag run.

    Parameters
    ----------
    dag_id : str
        The ID of the Dag.
    dag_run_id : str
        The ID of the Dag run.

    Returns
    -------
    dict
        A dictionary containing the task instances.
    """
    with get_client() as client:
        return client.list_task_instances(dag_id, dag_run_id)


@mcp.tool()
def get_task_instance(dag_id: str, dag_run_id: str, task_id: str) -> dict:
    """
    Get detailed information about a specific task instance.

    Parameters
    ----------
    dag_id : str
        The ID of the Dag.
    dag_run_id : str
        The ID of the Dag run.
    task_id : str
        The ID of the task.

    Returns
    -------
    dict
        A dictionary containing the task instance details.
    """
    with get_client() as client:
        return client.get_task_instance(dag_id, dag_run_id, task_id)


@mcp.tool()
def get_task_logs(dag_id: str, dag_run_id: str, task_id: str, task_try_number: int) -> dict:
    """
    Retrieve raw execution logs for a specific task instance try.

    Parameters
    ----------
    dag_id : str
        The ID of the Dag.
    dag_run_id : str
        The ID of the Dag run.
    task_id : str
        The ID of the task.
    task_try_number : int
        The try number of the task execution.

    Returns
    -------
    dict
        A dictionary containing the raw text log content.
    """
    with get_client() as client:
        return client.get_task_logs(dag_id, dag_run_id, task_id, task_try_number)
