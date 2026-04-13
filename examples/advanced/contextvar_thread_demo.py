# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Demonstrates that ContextVar values are NOT inherited by ThreadPoolExecutor
worker threads — confirming the correlation ID bug in _dispatch_chunks.

Run from repo root:
    .conda/python.exe examples/advanced/contextvar_thread_demo.py
"""

import threading
from contextvars import ContextVar, copy_context
from concurrent.futures import ThreadPoolExecutor

# Mirrors the SDK's _CALL_SCOPE_CORRELATION_ID exactly
CORRELATION_ID: ContextVar[str | None] = ContextVar("CORRELATION_ID", default=None)

def read_correlation_id(label: str) -> str:
    """Read the ContextVar — simulates what _RequestContext.from_request() does."""
    value = CORRELATION_ID.get()
    print(f"  [{label}] thread={threading.current_thread().name:20s}  "
          f"correlation_id = {value!r}")
    return value


# ---------------------------------------------------------------------------
# Part 1: WITHOUT fix — plain ThreadPoolExecutor.submit()
# ---------------------------------------------------------------------------
print("=" * 60)
print("PART 1: Plain submit() — no context propagation (current SDK)")
print("=" * 60)

CORRELATION_ID.set("abc-123-shared-id")
print(f"\nMain thread sets correlation_id = 'abc-123-shared-id'")
print(f"Dispatching 3 chunks to worker threads...\n")

with ThreadPoolExecutor(max_workers=3) as pool:
    futures = [pool.submit(read_correlation_id, f"chunk-{i}") for i in range(3)]
    results_before = [f.result() for f in futures]

print(f"\nMain thread still sees: {CORRELATION_ID.get()!r}")
print(f"Worker results: {results_before}")
print(f"\n=> All workers got None — correlation ID is LOST in concurrent path.\n")


# ---------------------------------------------------------------------------
# Part 2: WITH fix — copy_context().run()
# ---------------------------------------------------------------------------
print("=" * 60)
print("PART 2: copy_context() — correct propagation (proposed fix)")
print("=" * 60)

CORRELATION_ID.set("abc-123-shared-id")
print(f"\nMain thread sets correlation_id = 'abc-123-shared-id'")
print(f"Dispatching 3 chunks with ctx.run()...\n")

ctx = copy_context()  # snapshot the main thread's context
with ThreadPoolExecutor(max_workers=3) as pool:
    futures = [pool.submit(ctx.run, read_correlation_id, f"chunk-{i}") for i in range(3)]
    results_after = [f.result() for f in futures]

print(f"\nMain thread still sees: {CORRELATION_ID.get()!r}")
print(f"Worker results: {results_after}")
print(f"\n=> All workers got 'abc-123-shared-id' — correlation ID is preserved.\n")


# ---------------------------------------------------------------------------
# Part 3: Test the actual SDK _dispatch_chunks with the fix applied
# ---------------------------------------------------------------------------
print("=" * 60)
print("PART 3: Real SDK _dispatch_chunks (with fix applied)")
print("=" * 60)

from PowerPlatform.Dataverse.data._odata import (
    _dispatch_chunks,
    _CALL_SCOPE_CORRELATION_ID,
)

def simulate_chunk_request(chunk):
    """Reads the SDK's real ContextVar — same as _RequestContext.from_request()."""
    corr_id = _CALL_SCOPE_CORRELATION_ID.get()
    print(f"  chunk={chunk}  x-ms-correlation-id = {corr_id!r}  "
          f"(thread={threading.current_thread().name})")
    return corr_id

_CALL_SCOPE_CORRELATION_ID.set("real-sdk-call-uuid-xyz")
print(f"\nSDK: _call_scope sets correlation_id = 'real-sdk-call-uuid-xyz'")
print(f"SDK: _dispatch_chunks dispatches 3 chunks with max_workers=3\n")

chunks = ["chunk-A", "chunk-B", "chunk-C"]
results = _dispatch_chunks(simulate_chunk_request, chunks, max_workers=3)

print(f"\nResults: {results}")
if all(r == "real-sdk-call-uuid-xyz" for r in results):
    print("=> [PASS] All chunks received the correct correlation ID.")
else:
    print("=> [FAIL] Some chunks got None — fix not working.")
