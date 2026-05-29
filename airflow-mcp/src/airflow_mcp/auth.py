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

import logging
from functools import wraps
from typing import Any

import httpx
from fastmcp import Context
from fastmcp.server.auth.providers.jwt import JWTVerifier

log = logging.getLogger(__name__)


def _is_valid_url(url: str) -> bool:
    return url.startswith(("http://", "https://"))


class LoggingJWTVerifier(JWTVerifier):
    async def verify_token(self, token: str) -> Any:
        try:
            return await super().verify_token(token)
        except Exception as e:
            # Attempt to decode without verifying to extract sub, iss, aud
            try:
                import jwt

                unverified_claims = jwt.decode(token, options={"verify_signature": False})
                sub = unverified_claims.get("sub", "unknown")
                iss = unverified_claims.get("iss", "unknown")
                aud = unverified_claims.get("aud", "unknown")
            except Exception:
                sub = iss = aud = "unknown"

            log.warning("JWT verification failed for sub=%s iss=%s aud=%s: %s", sub, iss, aud, e)
            raise


def require_scope(scope: str):
    """
    Decorator to enforce that the caller's JWT has a specific scope.
    Must be applied to @mcp.tool() functions. The tool function must accept a `ctx: Context` parameter.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx = None
            for arg in args:
                if isinstance(arg, Context):
                    ctx = arg
                    break
            if not ctx:
                ctx = kwargs.get("ctx")

            if ctx is None:
                log.error("Context not found in arguments for require_scope.")
                raise ValueError("Context is required for require_scope")

            auth = getattr(ctx.request_context, "auth", None)
            if not auth:
                log.warning("Unauthenticated request to %s", func.__name__)
                raise PermissionError("Unauthenticated")

            scopes = auth.claims.get("scope", "")
            if isinstance(scopes, str):
                scopes = scopes.split()

            if scope not in scopes:
                log.warning("Missing required scope '%s' for %s", scope, func.__name__)
                raise PermissionError(f"Missing required scope: {scope}")

            return func(*args, **kwargs)

        return wrapper

    return decorator


def build_auth_verifier(
    *,
    jwks_url: str,
    audience: str,
    issuer: str,
    fetch_timeout: float = 5.0,
    max_retries: int = 3,
) -> JWTVerifier:
    """
    Build a JWT verifier that validates tokens against a JWKS endpoint.

    Uses FastMCP's built-in JWTVerifier which handles:
    - Async-safe JWKS key fetching with caching
    - RS256/ES256 and other asymmetric algorithm verification
    - Expiration, audience, and issuer claim validation
    - Full JWT payload extraction into AccessToken.claims

    Parameters
    ----------
    jwks_url:
        URL of the JWKS endpoint (e.g. ``https://idp.example.com/.well-known/jwks.json``).
    audience:
        Expected ``aud`` claim value. Tokens with a different audience are rejected.
    issuer:
        Expected ``iss`` claim value. Tokens from a different issuer are rejected.
    """
    if not _is_valid_url(jwks_url):
        raise ValueError(f"Invalid JWKS URL: {jwks_url}")

    transport = httpx.AsyncHTTPTransport(retries=max_retries)
    client = httpx.AsyncClient(timeout=fetch_timeout, transport=transport)

    return LoggingJWTVerifier(
        jwks_uri=jwks_url,
        audience=audience,
        issuer=issuer,
        http_client=client,
    )
