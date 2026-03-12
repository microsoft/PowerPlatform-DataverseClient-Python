"""Basic async usage example for the Dataverse SDK."""

import asyncio

# Use an async Azure Identity credential
# Install: pip install azure-identity
from azure.identity.aio import InteractiveBrowserCredential

from PowerPlatform.Dataverse.async_client import AsyncDataverseClient


async def main() -> None:
    credential = InteractiveBrowserCredential()

    async with AsyncDataverseClient("https://org.crm.dynamics.com", credential) as client:
        # Create a single record
        guid = await client.records.create("account", {"name": "Contoso"})
        print(f"[OK] Created account: {guid}")

        # Fetch the record back
        record = await client.records.get("account", guid, select=["name"])
        print(f"[OK] Name: {record['name']}")

        # Update the record
        await client.records.update("account", guid, {"telephone1": "555-0100"})
        print("[OK] Updated telephone")

        # Delete the record
        await client.records.delete("account", guid)
        print("[OK] Deleted account")

        # SQL query (async)
        rows = await client.query.sql("SELECT TOP 5 name FROM account ORDER BY name")
        for row in rows:
            print(f"  account: {row['name']}")

        # Multi-record fetch with async pagination
        print("[INFO] Paging through active accounts:")
        async for page in await client.records.get(
            "account",
            filter="statecode eq 0",
            select=["name"],
            page_size=20,
        ):
            for rec in page:
                print(f"  {rec['name']}")


if __name__ == "__main__":
    asyncio.run(main())
