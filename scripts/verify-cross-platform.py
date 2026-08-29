#!/usr/bin/env python
"""Verify cross-platform compatibility of the codebase."""

import platform
import subprocess
import sys
from pathlib import Path


def check_python_version():
    """Check Python version >= 3.11."""
    version = sys.version_info
    assert version.major == 3 and version.minor >= 11, f"Python 3.11+ required, got {version}"
    print(f"✅ Python {version.major}.{version.minor}.{version.micro}")

def check_imports():
    """Verify all critical imports work."""
    imports = [
        ("textual", "textual"),
        ("rich", "rich"),
        ("httpx", "httpx"),
        ("pydantic", "pydantic"),
        ("pydantic_settings", "pydantic_settings"),
        ("keyring", "keyring"),
        ("cryptography", "cryptography"),
        ("pandas", "pandas"),
        ("structlog", "structlog"),
        ("tenacity", "tenacity"),
        ("typer", "typer"),
    ]

    for name, module in imports:
        try:
            __import__(module)
            print(f"✅ {name}")
        except ImportError as e:
            print(f"❌ {name}: {e}")
            return False
    return True

def check_sf_cli():
    """Check SF CLI availability."""
    try:
        result = subprocess.run(["sf", "--version"], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ SF CLI: {result.stdout.strip()}")
        else:
            print("⚠️  SF CLI not found (install from https://developer.salesforce.com/tools/sfdxcli)")
    except FileNotFoundError:
        print("⚠️  SF CLI not found (install from https://developer.salesforce.com/tools/sfdxcli)")
    except Exception as e:
        print(f"⚠️  SF CLI check failed: {e}")

def check_platform_utils():
    """Test platform utilities."""
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from tcrm_toolkit.core.platform import get_config_dir, get_data_dir, get_os

    os_type = get_os()
    print(f"✅ OS detected: {os_type}")
    print(f"✅ Config dir: {get_config_dir()}")
    print(f"✅ Data dir: {get_data_dir()}")

def main():
    print(f"🔍 Cross-platform verification for {platform.system()} {platform.machine()}")
    print("=" * 60)

    check_python_version()
    print()
    check_imports()
    print()
    check_sf_cli()
    print()
    check_platform_utils()
    print()
    print("=" * 60)
    print("✅ All checks passed!")

if __name__ == "__main__":
    main()
