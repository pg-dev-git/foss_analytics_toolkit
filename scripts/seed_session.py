"""Seed live Salesforce session into TokenStore for E2E testing using environment variables."""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.core.auth.token_store import StoredToken, TokenStore


async def main() -> None:
    access_token = os.getenv("TCRM_ACCESS_TOKEN")
    instance_url = os.getenv("TCRM_INSTANCE_URL")
    username = os.getenv("TCRM_USERNAME", "default@example.com")

    if not access_token or not instance_url:
        print("❌ Please set TCRM_ACCESS_TOKEN and TCRM_INSTANCE_URL environment variables.")
        return

    crypto = create_crypto_manager()
    store = TokenStore(crypto)

    expires = (datetime.now(timezone.utc) + timedelta(days=365)).isoformat()

    token = StoredToken(
        access_token=access_token,
        instance_url=instance_url,
        username=username,
        alias="default",
        expires_at=expires,
    )

    await store.save_token(token)
    print(f"✅ Live session seeded successfully for user {username}!")


if __name__ == "__main__":
    asyncio.run(main())
