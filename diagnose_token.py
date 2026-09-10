#!/usr/bin/env python3
"""
Run this on your laptop to diagnose the token issue.
Compares SF CLI token with direct API test.
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from asftool.core.client import SalesforceClient
from asftool.core.config import Settings
from asftool.core.auth.sf_cli_auth import SFCLIAuthService
from asftool.core.crypto import create_crypto_manager


async def test_token(access_token: str, instance_url: str, label: str):
    """Test a token against the API."""
    print(f"\n{'='*60}")
    print(f"Testing: {label}")
    print(f"{'='*60}")
    print(f"Token (first 50): {access_token[:50]}...")
    print(f"Token length: {len(access_token)}")
    print(f"Instance URL: {instance_url}")
    
    # Check token format
    if access_token.startswith("00D"):
        print("Token format: Salesforce session ID (starts with 00D)")
    elif "!" in access_token:
        print("Token format: OAuth token (contains !)")
    else:
        print("Token format: Unknown")
    
    # Load settings from .env
    env_vars = {}
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env_vars[k] = v
    
    settings = Settings(
        encryption_key=env_vars.get("ENCRYPTION_KEY"),
        jwt_secret_key=env_vars.get("JWT_SECRET_KEY"),
        sf_api_version="v60.0",
        sf_default_domain="login.salesforce.com",
    )
    
    client = SalesforceClient(
        access_token=access_token,
        instance_url=instance_url,
        settings=settings,
    )
    
    try:
        # Test multiple endpoints
        endpoints = [
            ("/limits", "API Limits"),
            (f"{client.wave_base_url}/datasets", "Wave Datasets"),
            ("/query?q=SELECT+Id+FROM+User+LIMIT+1", "SOQL Query"),
        ]
        
        for path, name in endpoints:
            print(f"\n--- {name} ---")
            try:
                response = await client.get(path)
                print(f"Status: {response.status_code}")
                if response.status_code == 200:
                    print("✓ SUCCESS")
                else:
                    print(f"✗ FAILED: {response.text[:200]}")
            except Exception as e:
                print(f"✗ ERROR: {type(e).__name__}: {e}")
    finally:
        await client.close()


async def get_sf_cli_tokens():
    """Get all tokens from SF CLI."""
    print("\n" + "="*60)
    print("SF CLI Token Analysis")
    print("="*60)
    
    # 1. org display
    print("\n--- sf org display --target-org default --json ---")
    result = await asyncio.create_subprocess_exec(
        "sf", "org", "display", "--target-org", "default", "--json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await result.communicate()
    
    if result.returncode == 0:
        data = json.loads(stdout.decode())
        result_data = data.get("result", {})
        access_token = result_data.get("accessToken")
        instance_url = result_data.get("instanceUrl")
        username = result_data.get("username")
        expires_at = result_data.get("expirationDate")
        refresh_token = result_data.get("refreshToken")
        client_id = result_data.get("clientId")
        
        print(f"accessToken: {access_token[:50] if access_token else 'None'}...")
        print(f"instanceUrl: {instance_url}")
        print(f"username: {username}")
        print(f"expirationDate: {expires_at}")
        print(f"refreshToken: {refresh_token[:50] if refresh_token else 'None'}...")
        print(f"clientId: {client_id}")
        
        if access_token:
            await test_token(access_token, instance_url, "SF CLI org display")
    else:
        print(f"Failed: {stdout.decode()[:500]}")
    
    # 2. Check if there are other orgs
    print("\n--- sf org list --json ---")
    result = await asyncio.create_subprocess_exec(
        "sf", "org", "list", "--json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await result.communicate()
    
    if result.returncode == 0:
        data = json.loads(stdout.decode())
        orgs = data.get("result", {}).get("orgs", [])
        print(f"Found {len(orgs)} orgs:")
        for org in orgs:
            print(f"  - {org.get('alias', 'no-alias')}: {org.get('username', 'no-username')} ({org.get('instanceUrl', 'no-url')})")


async def test_via_app():
    """Test via the app's auth service."""
    print("\n" + "="*60)
    print("App Auth Service Test")
    print("="*60)
    
    env_vars = {}
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env_vars[k] = v
    
    settings = Settings(
        encryption_key=env_vars.get("ENCRYPTION_KEY"),
        jwt_secret_key=env_vars.get("JWT_SECRET_KEY"),
        sf_api_version="v60.0",
        sf_default_domain="login.salesforce.com",
    )
    crypto = create_crypto_manager()
    auth_service = SFCLIAuthService(settings, crypto)
    
    try:
        token = await auth_service.get_access_token(alias="default", auto_refresh=True)
        instance_url = await auth_service.get_instance_url(alias="default")
        print(f"App token (first 50): {token[:50]}...")
        await test_token(token, instance_url, "App Auth Service")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")


async def main():
    print("TOKEN DIAGNOSTIC TOOL")
    print("This will test tokens from SF CLI and the app against the API")
    
    await get_sf_cli_tokens()
    await test_via_app()
    
    print("\n" + "="*60)
    print("DIAGNOSIS")
    print("="*60)
    print("If SF CLI tokens fail but you can access the org in browser,")
    print("the SF CLI token may be a session ID, not an OAuth token.")
    print("Check if 'sf org display' returns a proper OAuth accessToken.")


if __name__ == "__main__":
    asyncio.run(main())
