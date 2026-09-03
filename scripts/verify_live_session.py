"""Verify live Salesforce session by listing datasets."""

import asyncio
from tcrm_toolkit.core.config import get_settings
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.interactive.session import SessionManager
from tcrm_toolkit.core.services.dataset_service import DatasetService


async def main() -> None:
    settings = get_settings()
    crypto = create_crypto_manager()
    session = SessionManager(settings=settings, crypto=crypto)

    await session.initialize()
    print(f"Current Org: {session.current_org}")

    if not session.current_org:
        print("❌ No active org session found.")
        return

    async with session.client_context() as client:
        service = DatasetService(client, settings)
        datasets = await service.list_datasets(page_size=10)
        print(f"✅ Successfully fetched {len(datasets)} datasets from live Salesforce org!")
        for ds in datasets:
            print(f" - [{ds.id}] {ds.label} ({ds.name})")


if __name__ == "__main__":
    asyncio.run(main())
