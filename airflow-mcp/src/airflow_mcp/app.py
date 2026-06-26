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

import os

from fastmcp import Context, FastMCP

from airflow_mcp.auth import build_auth_verifier
from airflow_mcp.client import AirflowClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Read credentials inside get_client() rather than at module level so that
# tests can override them via monkeypatch without import-order surprises.

_AIRFLOW_JWKS_URL = os.environ.get("AIRFLOW_JWKS_URL", "")
_AIRFLOW_JWKS_AUDIENCE = os.environ.get("AIRFLOW_JWKS_AUDIENCE", "airflow-mcp")
_AIRFLOW_JWKS_ISSUER = os.environ.get("AIRFLOW_JWKS_ISSUER", "")


def _create_mcp() -> FastMCP:
    """Create the FastMCP server instance, wiring JWT auth when configured."""
    if _AIRFLOW_JWKS_URL and _AIRFLOW_JWKS_ISSUER and _AIRFLOW_JWKS_AUDIENCE:
        auth = build_auth_verifier(
            jwks_url=_AIRFLOW_JWKS_URL,
            audience=_AIRFLOW_JWKS_AUDIENCE,
            issuer=_AIRFLOW_JWKS_ISSUER,
        )

        # Fail-fast JWKS health-check
        import logging
        import urllib.error
        import urllib.request

        try:
            req = urllib.request.Request(_AIRFLOW_JWKS_URL, headers={"User-Agent": "Airflow-MCP-Server"})
            with urllib.request.urlopen(req, timeout=5):
                pass
        except urllib.error.URLError as e:
            logging.getLogger(__name__).error(f"Failed to fetch JWKS from {_AIRFLOW_JWKS_URL}: {e}")
            raise SystemExit(1)
    else:
        auth = None

    return FastMCP("Airflow", auth=auth)


mcp = _create_mcp()


def get_client(ctx: Context | None = None) -> AirflowClient:
    """
    Return an AirflowClient backed by the service-account token.

    In HTTP mode the incoming JWT identifies the caller to *this* server via
    ``get_access_token().claims["sub"]``.  That identity is never forwarded to
    Airflow — all Airflow API calls use the ``AIRFLOW_MCP_API_TOKEN`` service
    account.  This is the no-passthrough, per-user-auth design.

    In stdio mode (e.g. Claude Desktop) there is no MCP auth layer and we
    fall through to the same service-account token, which is acceptable
    because stdio has no network exposure.
    """
    user_id = None
    if ctx and hasattr(ctx.request_context, "auth") and ctx.request_context.auth:
        user_id = ctx.request_context.auth.claims.get("sub")

    base_url = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080/api/v2")
    token = os.environ.get("AIRFLOW_MCP_API_TOKEN", "")
    return AirflowClient(base_url=base_url, token=token, user_id=user_id)


# Startup warning if token is missing
if not os.environ.get("AIRFLOW_MCP_API_TOKEN"):
    import logging

    logging.getLogger(__name__).warning(
        "AIRFLOW_MCP_API_TOKEN is missing or empty. API calls will fail with 401."
    )
