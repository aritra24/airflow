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
from airflow_mcp.auth import require_scope

from airflow_client.client.api.variable_api import VariableApi


@mcp.tool()
@require_scope("admin")
def list_variables(ctx: Context) -> dict:
    """
    List all available Airflow variables.

    This is an administrative tool that requires the 'admin' scope in the caller's JWT.

    Parameters
    ----------
    ctx : Context
        The MCP context injected by the server, used for authorization.

    Returns
    -------
    dict
        A dictionary containing the list of variables.
    """
    with get_client(ctx) as client:
        return VariableApi(client).get_variables().to_dict()
