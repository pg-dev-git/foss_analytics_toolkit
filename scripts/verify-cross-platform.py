#!/usr/bin/env python
"""Cross-platform verification script for ASFTool.

Run from the repo root:
    uv run python scripts/verify-cross-platform.py

Verifies the platform-independent primitives work on the current OS:
config directory creation, crypto round-trip, SF CLI availability, and
keyring backend resolution. Exits 0 on success, 1 on failure.
"""

import asyncio
import platform
import sys
from pathlib import Path

# Ensure the repo root is importable when run as a loose script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from asftool.core.auth import SFCLIAuthService  # noqa: E402
from asftool.core.config import get_settings  # noqa: E402
from asftool.core.crypto import create_crypto_manager  # noqa: E402


async def verify() -> int:
    """Run cross-platform verification checks."""
    print("=" * 60)
    print("ASFTool Cross-Platform Verification")
    print("=" * 60)
    print(f"Platform: {platform.platform()}")
    print(f"Python: {sys.version}")
    print()

    checks: list[tuple[str, bool]] = []

    # Config directory is creatable/writable
    settings = get_settings()
    config_dir = settings.config_dir
    config_dir.mkdir(parents=True, exist_ok=True)
    checks.append(("Config directory", config_dir.exists()))

    # Crypto round-trip
    crypto = create_crypto_manager()
    test_data = "test secret"
    encrypted = crypto.encrypt(test_data)
    decrypted = crypto.decrypt(encrypted)
    checks.append(("Crypto roundtrip", decrypted == test_data))

    # SF CLI availability
    auth = SFCLIAuthService(settings, crypto)
    sf_cli_ok = auth.sf_cli.is_available()
    checks.append(("SF CLI available", sf_cli_ok))
    if not sf_cli_ok:
        print("  INFO: SF CLI not found — install from")
        print("  https://developer.salesforce.com/tools/sfdxcli")

    # Keyring backend resolution
    try:
        import keyring

        kr = keyring.get_keyring()
        checks.append(("Keyring backend", True))
        name = getattr(kr, "name", None) or kr.__class__.__name__
        print(f"  Keyring: {name}")
    except Exception as e:
        checks.append(("Keyring backend", False))
        print(f"  Keyring error: {e}")

    # Results
    print()
    print("Results:")
    all_pass = True
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}")

    print()
    if all_pass:
        print("All verification checks PASSED")
        return 0
    print("Some verification checks FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(verify()))
