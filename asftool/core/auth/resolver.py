"""Cascading Auth Resolver supporting env, keyring, SF CLI, JWT/Device."""
from typing import Any

from asftool.core.auth.token_store import TokenStore
from asftool.core.auth.sf_cli_auth import SFCLIAuthService
from asftool.core.auth.pkce_auth import WebPKCEAuthService
from asftool.core.auth.jwt_auth import JWTAuthService
from asftool.core.auth.device_auth import DeviceAuthService
from asftool.core.config import Settings, get_settings
from asftool.core.crypto import create_crypto_manager
import os

class AuthResolver:
    """Resolution hierarchy: Env -> Keyring -> SF CLI -> JWT/Device."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.crypto = create_crypto_manager()
        self.token_store = TokenStore(self.crypto)
        self.sf_cli_auth = SFCLIAuthService(self.settings, self.crypto)
        self.pkce_auth = WebPKCEAuthService(self.settings, self.crypto)
        self.jwt_auth = JWTAuthService(self.settings)
        self.device_auth = DeviceAuthService(self.settings)

    async def resolve_token(self, alias: str = "default") -> str | None:
        # 1. Environment variables
        env_token = os.environ.get("ASFTOOL_ACCESS_TOKEN")
        env_url = os.environ.get("ASFTOOL_INSTANCE_URL")
        if env_token and env_url:
            return env_token

        # 2. Keyring / Encrypted Token Store
        token = await self.token_store.load_token(alias)
        if token and not token.is_expired():
            return token.access_token

        # 3. SF CLI Session Sync
        if self.sf_cli_auth.sf_cli.is_available():
            try:
                return await self.sf_cli_auth.get_access_token(alias=alias, auto_refresh=True)
            except Exception:
                pass

        # 4. Optional JWT / Device Flow
        return None

    async def resolve_instance_url(self, alias: str = "default") -> str | None:
        url = os.environ.get("ASFTOOL_INSTANCE_URL")
        if url:
            return url
        token = await self.token_store.load_token(alias)
        if token and token.instance_url:
            return token.instance_url
        try:
            return await self.sf_cli_auth.get_instance_url(alias=alias)
        except Exception:
            return None
