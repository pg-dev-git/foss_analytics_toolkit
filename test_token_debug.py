#!/usr/bin/env python3
"""
Comprehensive token debugging script.
Run this to compare SF CLI token vs .env token directly against Salesforce API.
"""
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from asftool.core.client import SalesforceClient, create_client
from asftool.core.config import Settings
from asftool.core.auth.sf_cli_auth import SFCLIAuthService
from asftool.core.crypto import create_crypto_manager


def load_env():
    """Load .env file manually."""
    env_vars = {}
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env_vars[k] = v
    return env_vars


async def test_token_directly(access_token: str, instance_url: str, label: str, settings: Settings):
    """Test a token directly against the API."""
    print(f"\n{'='*60}")
    print(f"Testing {label}")
    print(f"{'='*60}")
    print(f"Token (first 40): {access_token[:40]}...")
    print(f"Instance URL: {instance_url}")
    
    client = SalesforceClient(
        access_token=access_token,
        instance_url=instance_url,
        settings=settings,
    )
    
    try:
        # Test 1: List datasets
        print(f"\n--- Test 1: List datasets ---")
        response = await client.get(f"{client.wave_base_url}/datasets")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Success! Found {len(data.get('datasets', []))} datasets")
        else:
            print(f"Response: {response.text[:500]}")
        
        # Test 2: Get limits (simpler endpoint)
        print(f"\n--- Test 2: Get limits ---")
        response = await client.get("/limits")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("Success!")
        else:
            print(f"Response: {response.text[:500]}")
            
        # Test 3: Query (SOQL)
        print(f"\n--- Test 3: Simple SOQL query ---")
        response = await client.get("/query", params={"q": "SELECT Id FROM User LIMIT 1"})
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("Success!")
        else:
            print(f"Response: {response.text[:500]}")
            
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()


async def get_sf_cli_token():
    """Get token from SF CLI using org display."""
    print("\n" + "="*60)
    print("Getting token from SF CLI (sf org display)")
    print("="*60)
    
    result = await asyncio.create_subprocess_exec(
        "sf", "org", "display", "--target-org", "default", "--json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await result.communicate()
    
    print(f"Return code: {result.returncode}")
    if stderr:
        print(f"Stderr: {stderr.decode()[:500]}")
    
    if result.returncode == 0:
        data = json.loads(stdout.decode())
        result_data = data.get("result", {})
        access_token = result_data.get("accessToken")
        instance_url = result_data.get("instanceUrl")
        username = result_data.get("username")
        expires_at = result_data.get("expirationDate")
        refresh_token = result_data.get("refreshToken")
        
        print(f"Access token (first 40): {access_token[:40] if access_token else 'None'}...")
        print(f"Instance URL: {instance_url}")
        print(f"Username: {username}")
        print(f"Expires at: {expires_at}")
        print(f"Refresh token (first 40): {refresh_token[:40] if refresh_token else 'None'}...")
        
        return access_token, instance_url
    else:
        print(f"Failed: {stdout.decode()[:500]}")
        return None, None


async def get_sf_cli_login_token():
    """Get token from SF CLI using org login web (if not already logged in)."""
    print("\n" + "="*60)
    print("Getting token from SF CLI (sf org login web)")
    print("="*60)
    
    # First logout to force fresh login
    await asyncio.create_subprocess_exec(
        "sf", "org", "logout", "--target-org", "default", "--json", "--no-prompt",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    
    result = await asyncio.create_subprocess_exec(
        "sf", "org", "login", "web", "--alias", "default", "--json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await result.communicate()
    
    print(f"Return code: {result.returncode}")
    if stderr:
        print(f"Stderr: {stderr.decode()[:500]}")
    
    if result.returncode == 0:
        data = json.loads(stdout.decode())
        result_data = data.get("result", {})
        access_token = result_data.get("accessToken")
        instance_url = result_data.get("instanceUrl")
        username = result_data.get("username")
        expires_at = result_data.get("expirationDate")
        refresh_token = result_data.get("refreshToken")
        
        print(f"Access token (first 40): {access_token[:40] if access_token else 'None'}...")
        print(f"Instance URL: {instance_url}")
        print(f"Username: {username}")
        print(f"Expires at: {expires_at}")
        print(f"Refresh token (first 40): {refresh_token[:40] if refresh_token else 'None'}...")
        
        return access_token, instance_url
    else:
        print(f"Failed: {stdout.decode()[:500]}")
        return None, None


async def test_via_auth_service():
    """Test via the app's auth service."""
    print("\n" + "="*60)
    print("Testing via SFCLIAuthService")
    print("="*60)
    
    env_vars = load_env()
    settings = Settings(
        encryption_key=env_vars.get("ENCRYPTION_KEY"),
        jwt_secret_key=env_vars.get("JWT_SECRET_KEY"),
        sf_api_version="v60.0",
        sf_default_domain="login.salesforce.com",
    )
    crypto = create_crypto_manager()
    auth_service = SFCLIAuthService(settings, crypto)
    
    # Get token
    print("Calling get_access_token()...")
    try:
        token = await auth_service.get_access_token(alias="default", auto_refresh=True)
        print(f"Token (first 40): {token[:40]}...")
        
        instance_url = await auth_service.get_instance_url(alias="default")
        print(f"Instance URL: {instance_url}")
        
        # Test it
        await test_token_directly(token, instance_url, "SFCLIAuthService token", settings)
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


async def main():
    env_vars = load_env()
    
    # Create settings
    settings = Settings(
        encryption_key=env_vars.get("ENCRYPTION_KEY"),
        jwt_secret_key=env_vars.get("JWT_SECRET_KEY"),
        sf_api_version="v60.0",
        sf_default_domain="login.salesforce.com",
    )
    
    instance_url = "https://claritev6-dev-ed.develop.my.salesforce.com"
    
    # Test 1: .env token (known working)
    env_token = env_vars.get("SF_AUTH_TOKEN")
    if env_token:
        await test_token_directly(env_token, instance_url, ".env SF_AUTH_TOKEN", settings)
    else:
        print("No SF_AUTH_TOKEN in .env")
    
    # Test 2: SF CLI org display token
    cli_token, cli_instance = await get_sf_cli_token()
    if cli_token:
        await test_token_directly(cli_token, cli_instance or instance_url, "SF CLI org display", settings)
    
    # Test 3: SF CLI login web token (optional - requires browser)
    # Uncomment to test fresh login:
    # login_token, login_instance = await get_sf_cli_login_token()
    # if login_token:
    #     await test_token_directly(login_token, login_instance or instance_url, "SF CLI login web", settings)
    
    # Test 4: Via auth service
    await test_via_auth_service()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("Compare the tokens above. The .env token should work.")
    print("If SF CLI tokens fail, the issue is with SF CLI's token store.")
    print("If all tokens work, the issue is in the app's token handling.")


if __name__ == "__main__":
    asyncio.run(main())
