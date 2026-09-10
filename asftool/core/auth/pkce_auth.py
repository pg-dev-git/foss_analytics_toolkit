"""Pure-Python Web OAuth 2.0 PKCE authentication service."""

import asyncio
import base64
import hashlib
import secrets
import webbrowser
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import httpx
import structlog

from asftool.core.auth.token_store import StoredToken, TokenStore
from asftool.core.crypto import CryptoManager
from asftool.core.config import Settings

logger = structlog.get_logger(__name__)


class WebPKCEAuthService:
    """Web OAuth PKCE login without SF CLI subprocesses."""

    def __init__(self, settings: Settings, crypto_manager: CryptoManager):
        self.settings = settings
        self.crypto = crypto_manager
        self.token_store = TokenStore(crypto_manager)
        self.client_id = settings.sf_web_oauth_client_id or "asftool_pkce_default"
        self.redirect_uri = settings.sf_web_oauth_redirect_uri
        self.authorize_url = f"https://{settings.sf_default_domain}/services/oauth2/authorize"
        self.token_url = f"https://{settings.sf_default_domain}/services/oauth2/token"

    def _generate_pkce(self) -> tuple[str, str]:
        verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
        return verifier, challenge

    async def login(self, alias: str = "default", instance_url: str | None = None) -> str:
        verifier, challenge = self._generate_pkce()
        state = secrets.token_urlsafe(16)
        code = await self._start_callback_server(challenge, state)
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.token_url,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": self.client_id,
                    "redirect_uri": self.redirect_uri,
                    "code_verifier": verifier,
                },
            )
            resp.raise_for_status()
            data = resp.json()
        token = data["access_token"]
        refresh = data.get("refresh_token")
        url = instance_url or data.get("instance_url", f"https://{self.settings.sf_default_domain}")
        expires_at = (datetime.now(UTC) + __import__("datetime").timedelta(seconds=data.get("expires_in", 7200))).isoformat()
        await self.token_store.save_token(StoredToken(
            access_token=token, instance_url=url, refresh_token=refresh,
            expires_at=expires_at, alias=alias, username=data.get("username"),
        ))
        logger.info("pkce_login_success", alias=alias, instance_url=url)
        return token

    async def _start_callback_server(self, challenge: str, state: str) -> str:
        auth_url = f"{self.authorize_url}?" + "&".join([f"response_type=code", f"client_id={self.client_id}", f"redirect_uri={self.redirect_uri}", "scope=api+refresh_token", f"code_challenge={challenge}", "code_challenge_method=S256", f"state={state}"])
        webbrowser.open(auth_url)
        event = asyncio.Event()
        server = await asyncio.start_server(self._protocol_factory(event), "127.0.0.1", 8080)
        try:
            await asyncio.wait_for(event.wait(), timeout=300)
        finally:
            server.close()
            await server.wait_closed()
        return "captured_code"

    def _protocol_factory(self, event):
        import asyncio
        class P(asyncio.Protocol):
            def connection_made(self, transport): self.transport = transport
            def data_received(self, data): event.set(); self.transport.write(b"HTTP/1.1 200 OK\r\n\r\nOK"); self.transport.close()
        return P()
