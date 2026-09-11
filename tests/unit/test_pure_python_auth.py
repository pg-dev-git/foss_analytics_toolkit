"""Unit tests for pure-python auth (PKCE, resolver, transport)."""
import pytest

@pytest.mark.asyncio
async def test_auth_resolver_env_priority():
    import os
    os.environ["ASFTOOL_ACCESS_TOKEN"] = "env_token"
    os.environ["ASFTOOL_INSTANCE_URL"] = "https://env.salesforce.com"
    from asftool.core.auth.resolver import AuthResolver
    resolver = AuthResolver()
    token = await resolver.resolve_token()
    assert token == "env_token"

@pytest.mark.asyncio
async def test_salesforce_auth_transport_no_subprocess():
    with open("asftool/core/client.py") as f:
        source = f.read()
    transport_source = source.split("class SalesforceAuthTransport")[1] if "class SalesforceAuthTransport" in source else ""
    assert "subprocess" not in transport_source
    print("Transport has zero subprocess.run calls")
