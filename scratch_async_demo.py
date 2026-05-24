"""
Demo: async-only SDK usage patterns and asyncio.run() behavior.

Run with:
    PYTHONPATH=src python scratch_async_demo.py
"""

import asyncio


# ---------------------------------------------------------------------------
# Pretend this is an "async-only SDK" — no sync client exists
# ---------------------------------------------------------------------------

async def sdk_create_record(table: str, data: dict) -> str:
    """Simulated async-only SDK method."""
    await asyncio.sleep(0)  # yields control, like a real network call
    return f"fake-guid-for-{data.get('name', 'unknown')}"


# ---------------------------------------------------------------------------
# Part 1: How sync users must call an async-only SDK
# ---------------------------------------------------------------------------

print("=" * 60)
print("PART 0: Calling async function directly (no await, no asyncio.run)")
print("=" * 60)

result = sdk_create_record("account", {"name": "Contoso"})
print(f"Return value: {result}")
print(f"Type:         {type(result)}")
print("The coroutine was never executed — no network call happened.")
print()

print("=" * 60)
print("PART 1: Sync usage of an async-only function")
print("=" * 60)

# Sync user wraps every call in asyncio.run()
record_id = asyncio.run(sdk_create_record("account", {"name": "Contoso"}))
print(f"Created record: {record_id}")
print()
print("Sync call required:")
print("  asyncio.run(client.records.create('account', {'name': 'Contoso'}))")
print("Instead of the simpler sync SDK syntax:")
print("  client.records.create('account', {'name': 'Contoso'})")
print()


# ---------------------------------------------------------------------------
# Part 2: asyncio.run() raises RuntimeError when a loop is already running
#         (simulates Jupyter notebook / FastAPI endpoint / pytest-asyncio)
# ---------------------------------------------------------------------------

print("=" * 60)
print("PART 2: asyncio.run() inside a running event loop")
print("        (simulates Jupyter / FastAPI / pytest-asyncio)")
print("=" * 60)

async def simulate_jupyter_cell():
    """
    Jupyter runs all cells inside a single persistent event loop.
    Any code in a cell is already inside that running loop.
    A sync user who tries asyncio.run() here hits RuntimeError.
    """
    print("Inside running event loop (simulating a Jupyter cell)...")

    # This is what a sync user would try:
    try:
        result = asyncio.run(sdk_create_record("account", {"name": "Fabrikam"}))
        print(f"  [unexpected success] {result}")
    except RuntimeError as e:
        print(f"  RuntimeError raised: {e}")

    print()
    print("  The only way to call the SDK here is with await:")
    result = await sdk_create_record("account", {"name": "Fabrikam"})
    print(f"  await result: {result}")
    print()
    print("  So even 'sync' users in Jupyter must learn async/await.")

asyncio.run(simulate_jupyter_cell())
