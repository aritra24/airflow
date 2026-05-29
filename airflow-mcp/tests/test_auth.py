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
"""
Tests for build_auth_verifier / JWTVerifier.

FastMCP's JWTVerifier uses ``joserfc`` for JWT operations and fetches JWKS via
its own async ``_fetch_jwks`` method.  Tests mock that method to avoid real
HTTP requests and use FastMCP's own ``RSAKeyPair`` helper to produce correctly
signed tokens.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from airflow_mcp.auth import build_auth_verifier
from fastmcp.server.auth.providers.jwt import RSAKeyPair

# ---------------------------------------------------------------------------
# Shared test key pair (generated once per module)
# ---------------------------------------------------------------------------
_KEY_PAIR = RSAKeyPair.generate()


def _jwks_response() -> dict:
    """Return a minimal JWKS document containing the test public key."""
    from joserfc import jwk as jose_jwk

    # Re-derive the public JWK from the PEM stored in the key pair
    key = jose_jwk.import_key(_KEY_PAIR.public_key.encode(), "RSA")
    jwk_dict = key.as_dict()
    jwk_dict["kid"] = "test-kid"
    return {"keys": [jwk_dict]}


def _make_token(**claim_overrides) -> str:
    """Sign a test JWT with the module-level key pair."""
    return _KEY_PAIR.create_token(
        subject=claim_overrides.pop("sub", "alice"),
        issuer=claim_overrides.pop("iss", "https://idp.example.com"),
        audience=claim_overrides.pop("aud", "airflow-mcp"),
        kid="test-kid",
        additional_claims=claim_overrides or None,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture
def verifier():
    return build_auth_verifier(
        jwks_url="http://localhost:8080/auth/jwks",
        audience="airflow-mcp",
        issuer="https://idp.example.com",
    )


@pytest.mark.asyncio
async def test_verifier_accepts_valid_token(verifier):
    """A correctly signed token with matching audience and issuer is accepted."""
    token = _make_token()

    with patch.object(verifier, "_fetch_jwks", new=AsyncMock(return_value=_jwks_response())):
        result = await verifier.verify_token(token)

    assert result is not None
    assert result.claims.get("sub") == "alice"
    assert result.claims.get("iss") == "https://idp.example.com"


@pytest.mark.asyncio
async def test_verifier_rejects_wrong_audience(verifier):
    """A token with the wrong audience claim is rejected."""
    token = _make_token(aud="wrong-audience")

    with patch.object(verifier, "_fetch_jwks", new=AsyncMock(return_value=_jwks_response())):
        result = await verifier.verify_token(token)

    assert result is None


@pytest.mark.asyncio
async def test_verifier_rejects_wrong_issuer(verifier):
    """A token from an unexpected issuer is rejected."""
    token = _make_token(iss="https://evil.example.com")

    with patch.object(verifier, "_fetch_jwks", new=AsyncMock(return_value=_jwks_response())):
        result = await verifier.verify_token(token)

    assert result is None


@pytest.mark.asyncio
async def test_verifier_rejects_expired_token(verifier):
    """An expired token is rejected."""
    # RSAKeyPair.create_token does not let us pass a negative expires_in, so
    # create an already-expired token via a very short window and time-shift.

    token = _KEY_PAIR.create_token(
        subject="alice",
        issuer="https://idp.example.com",
        audience="airflow-mcp",
        kid="test-kid",
        expires_in_seconds=-1,  # already expired
    )

    with patch.object(verifier, "_fetch_jwks", new=AsyncMock(return_value=_jwks_response())):
        result = await verifier.verify_token(token)

    assert result is None


@pytest.mark.asyncio
async def test_verifier_claims_populated(verifier):
    """The AccessToken returned has the full JWT payload in .claims."""
    token = _make_token()

    with patch.object(verifier, "_fetch_jwks", new=AsyncMock(return_value=_jwks_response())):
        result = await verifier.verify_token(token)

    assert result is not None
    # Must have at minimum: sub, iss, aud, exp, iat
    for claim in ("sub", "iss", "aud", "exp", "iat"):
        assert claim in result.claims, f"missing claim: {claim}"


@pytest.mark.asyncio
async def test_verifier_malformed_jwks(verifier):
    """If the JWKS document is malformed, verification returns None."""
    token = _make_token()
    with patch.object(verifier, "_fetch_jwks", new=AsyncMock(return_value={"not_keys": []})):
        result = await verifier.verify_token(token)
        assert result is None


@pytest.mark.asyncio
async def test_verifier_network_error(verifier):
    """If the JWKS fetch fails, verification raises or returns None."""
    token = _make_token()
    with patch.object(verifier, "_fetch_jwks", new=AsyncMock(side_effect=Exception("Network error"))):
        with pytest.raises(Exception):
            await verifier.verify_token(token)


def test_require_scope_decorator():
    from airflow_mcp.auth import require_scope
    from unittest.mock import MagicMock

    class MockAccessToken:
        def __init__(self, claims):
            self.claims = claims

    @require_scope("admin")
    def dummy_tool(ctx):
        return "success"

    # Context without auth
    ctx_no_auth = MagicMock()
    ctx_no_auth.request_context.auth = None
    with pytest.raises(PermissionError, match="Unauthenticated"):
        dummy_tool(ctx=ctx_no_auth)

    # Context with auth but no admin scope
    token_no_scope = MockAccessToken(claims={"scope": "read"})
    ctx_no_scope = MagicMock()
    ctx_no_scope.request_context.auth = token_no_scope
    with pytest.raises(PermissionError, match="Missing required scope"):
        dummy_tool(ctx=ctx_no_scope)

    # Context with admin scope
    token_admin = MockAccessToken(claims={"scope": "admin read"})
    ctx_admin = MagicMock()
    ctx_admin.request_context.auth = token_admin
    assert dummy_tool(ctx=ctx_admin) == "success"
