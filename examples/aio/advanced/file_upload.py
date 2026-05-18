# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Async File Upload Example

Async equivalent of examples/advanced/file_upload.py.

This example demonstrates file upload capabilities using the async
PowerPlatform-Dataverse-Client SDK with automatic chunking for large files.

Prerequisites:
    pip install PowerPlatform-Dataverse-Client
    pip install azure-identity
"""

import asyncio
import hashlib
import sys
import traceback
from pathlib import Path

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _auth import AsyncInteractiveBrowserCredential

ATTRIBUTE_VISIBILITY_DELAYS = (0, 3, 10, 20, 35, 50, 70, 90, 120)

# --- Helpers ---

_FILE_HASH_CACHE: dict = {}


def file_sha256(path: Path):
    """Return (hex_digest, size_bytes) for the file, with caching."""
    try:
        cached = _FILE_HASH_CACHE.get(path)
        if cached:
            return cached
        h = hashlib.sha256()
        size = 0
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                size += len(chunk)
                h.update(chunk)
        result = (h.hexdigest(), size)
        _FILE_HASH_CACHE[path] = result
        return result
    except Exception:  # noqa: BLE001
        return None, None


def generate_test_file(size_mb: int = 10) -> Path:
    """Generate a dummy text file of the specified size for testing."""
    test_file = Path(__file__).resolve().parent / f"test_dummy_{size_mb}mb.txt"
    target_size = size_mb * 1024 * 1024

    line = b"The quick brown fox jumps over the lazy dog. " * 2 + b"\n"
    with test_file.open("wb") as f:
        written = 0
        while written < target_size:
            chunk = line * min(1000, (target_size - written) // len(line) + 1)
            chunk = chunk[: target_size - written]
            f.write(chunk)
            written += len(chunk)

    print({"test_file_generated": str(test_file), "size_mb": test_file.stat().st_size / (1024 * 1024)})
    return test_file


async def backoff(coro_fn, *, delays=(0, 2, 5, 10, 20, 20)):
    """Retry an async operation with exponential back-off."""
    last = None
    total_delay = 0
    attempts = 0
    for d in delays:
        if d:
            await asyncio.sleep(d)
            total_delay += d
        attempts += 1
        try:
            result = await coro_fn()
            if attempts > 1:
                retry_count = attempts - 1
                print(f"   [INFO] Backoff succeeded after {retry_count} retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:  # noqa: BLE001
            last = ex
            continue
    if last:
        if attempts:
            retry_count = max(attempts - 1, 0)
            print(f"   [WARN] Backoff exhausted after {retry_count} retry(s); waited {total_delay}s total.")
        raise last


# --- Table ensure ---
TABLE_SCHEMA_NAME = "new_FileSample"


async def ensure_table(client) -> dict:
    """Get or create the demo table."""
    existing = await backoff(lambda: client.tables.get(TABLE_SCHEMA_NAME))
    if existing:
        print({"table": TABLE_SCHEMA_NAME, "existed": True})
        return existing
    info = await backoff(lambda: client.tables.create(TABLE_SCHEMA_NAME, {"new_Title": "string"}))
    print({"table": TABLE_SCHEMA_NAME, "existed": False, "metadata_id": info.get("metadata_id")})
    return info


# --- Main ---


async def main():
    entered = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not entered:
        print("No URL entered; exiting.")
        sys.exit(1)
    base_url = entered.rstrip("/")

    # Mode selection (numeric):
    # 1 = small (single PATCH <128MB)
    # 2 = chunk (streaming for any size)
    # 3 = all (small + chunk)
    mode_raw = input("Choose mode: 1) small  2) chunk  3) all [default 3]: ").strip()
    if not mode_raw:
        mode_raw = "3"
    if mode_raw not in {"1", "2", "3"}:
        print({"invalid_mode": mode_raw, "fallback": 3})
        mode_raw = "3"
    mode_int = int(mode_raw)
    run_small = mode_int in (1, 3)
    run_chunk = mode_int in (2, 3)

    delete_record_choice = input("Delete the created record at end? (Y/n): ").strip() or "y"
    cleanup_record = delete_record_choice.lower() in ("y", "yes", "true", "1")

    delete_table_choice = input("Delete the table at end? (y/N): ").strip() or "n"
    cleanup_table = delete_table_choice.lower() in ("y", "yes", "true", "1")

    credential = AsyncInteractiveBrowserCredential()

    # Generate test files before entering the async context
    generated_10mb = generate_test_file(10)
    generated_8mb = generate_test_file(8)

    try:
        async with AsyncDataverseClient(base_url=base_url, credential=credential) as client:

            # --------------------------- Table ensure ---------------------------
            try:
                table_info = await ensure_table(client)
            except Exception:  # noqa: BLE001
                print("Table ensure failed:")
                traceback.print_exc()
                sys.exit(1)

            entity_set = table_info.get("entity_set_name")
            table_schema_name = table_info.get("table_schema_name")
            attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
            name_attr = f"{attr_prefix}_name"
            small_file_attr_schema = f"{attr_prefix}_SmallDocument"
            chunk_file_attr_schema = f"{attr_prefix}_ChunkDocument"

            # --------------------------- Record create ---------------------------
            record_id = None
            try:
                payload = {name_attr: "Async File Sample Record"}
                print({"call": f"client.records.create('{table_schema_name}', payload)"})
                record_id = await backoff(lambda: client.records.create(table_schema_name, payload))
                print({"record_created": True, "id": record_id, "table schema name": table_schema_name})
            except Exception as e:  # noqa: BLE001
                print({"record_created": False, "error": str(e)})
                sys.exit(1)

            if not record_id:
                print("No record id; aborting upload.")
                sys.exit(1)

            # --------------------------- Small single-request upload ---------------------------
            if run_small:
                print("Small single-request upload demo:")
                try:
                    src_hash, small_file_size = file_sha256(generated_10mb)

                    await backoff(
                        lambda: client.files.upload(
                            table=table_schema_name,
                            record_id=record_id,
                            file_column=small_file_attr_schema,
                            path=str(generated_10mb),
                            mode="small",
                        )
                    )
                    print({"small_upload_completed": True, "small_source_size": small_file_size})

                    # Download and verify via internal OData client
                    async with client._scoped_odata() as od:
                        dl_url = f"{od.api}/{entity_set}({record_id})/{small_file_attr_schema.lower()}/$value"
                        resp = await od._request("get", dl_url)
                        content = await resp.read() if hasattr(resp, "read") else (resp.content or b"")

                    downloaded_hash = hashlib.sha256(content).hexdigest() if content else None
                    hash_match = (downloaded_hash == src_hash) if (downloaded_hash and src_hash) else None
                    print(
                        {
                            "small_file_source_size": small_file_size,
                            "small_file_download_size": len(content),
                            "small_file_size_match": len(content) == small_file_size,
                            "small_file_source_sha256_prefix": src_hash[:16] if src_hash else None,
                            "small_file_download_sha256_prefix": downloaded_hash[:16] if downloaded_hash else None,
                            "small_file_hash_match": hash_match,
                        }
                    )

                    # Replace with 8MB file
                    print("Small single-request upload demo - REPLACE with 8MB file:")
                    replace_hash, replace_size = file_sha256(generated_8mb)
                    await backoff(
                        lambda: client.files.upload(
                            table=table_schema_name,
                            record_id=record_id,
                            file_column=small_file_attr_schema,
                            path=str(generated_8mb),
                            mode="small",
                            if_none_match=False,
                        )
                    )
                    print({"small_replace_upload_completed": True, "small_replace_source_size": replace_size})

                    async with client._scoped_odata() as od:
                        dl_url = f"{od.api}/{entity_set}({record_id})/{small_file_attr_schema.lower()}/$value"
                        resp_r = await od._request("get", dl_url)
                        content_r = await resp_r.read() if hasattr(resp_r, "read") else (resp_r.content or b"")

                    dl_hash_r = hashlib.sha256(content_r).hexdigest() if content_r else None
                    hash_match_r = (dl_hash_r == replace_hash) if (dl_hash_r and replace_hash) else None
                    print(
                        {
                            "small_replace_source_size": replace_size,
                            "small_replace_download_size": len(content_r),
                            "small_replace_size_match": len(content_r) == replace_size,
                            "small_replace_source_sha256_prefix": replace_hash[:16] if replace_hash else None,
                            "small_replace_download_sha256_prefix": dl_hash_r[:16] if dl_hash_r else None,
                            "small_replace_hash_match": hash_match_r,
                        }
                    )
                except Exception as ex:  # noqa: BLE001
                    print({"single_upload_failed": str(ex)})

            # --------------------------- Chunk (streaming) upload ---------------------------
            if run_chunk:
                print("Streaming chunk upload demo (mode='chunk'):")
                try:
                    src_hash_chunk, src_size_chunk = file_sha256(generated_10mb)

                    await backoff(
                        lambda: client.files.upload(
                            table=table_schema_name,
                            record_id=record_id,
                            file_column=chunk_file_attr_schema,
                            path=str(generated_10mb),
                            mode="chunk",
                        )
                    )
                    print({"chunk_upload_completed": True})

                    async with client._scoped_odata() as od:
                        dl_url = f"{od.api}/{entity_set}({record_id})/{chunk_file_attr_schema.lower()}/$value"
                        resp = await od._request("get", dl_url)
                        content_chunk = await resp.read() if hasattr(resp, "read") else (resp.content or b"")

                    dst_hash_chunk = hashlib.sha256(content_chunk).hexdigest() if content_chunk else None
                    hash_match_chunk = (
                        (dst_hash_chunk == src_hash_chunk) if (dst_hash_chunk and src_hash_chunk) else None
                    )
                    print(
                        {
                            "chunk_source_size": src_size_chunk,
                            "chunk_download_size": len(content_chunk),
                            "chunk_size_match": len(content_chunk) == src_size_chunk,
                            "chunk_source_sha256_prefix": src_hash_chunk[:16] if src_hash_chunk else None,
                            "chunk_download_sha256_prefix": dst_hash_chunk[:16] if dst_hash_chunk else None,
                            "chunk_hash_match": hash_match_chunk,
                        }
                    )

                    # Replace with 8MB file
                    print("Streaming chunk upload demo - REPLACE with 8MB file:")
                    replace_hash_c, replace_size_c = file_sha256(generated_8mb)
                    await backoff(
                        lambda: client.files.upload(
                            table=table_schema_name,
                            record_id=record_id,
                            file_column=chunk_file_attr_schema,
                            path=str(generated_8mb),
                            mode="chunk",
                            if_none_match=False,
                        )
                    )
                    print({"chunk_replace_upload_completed": True})

                    async with client._scoped_odata() as od:
                        dl_url = f"{od.api}/{entity_set}({record_id})/{chunk_file_attr_schema.lower()}/$value"
                        resp_rc = await od._request("get", dl_url)
                        content_rc = await resp_rc.read() if hasattr(resp_rc, "read") else (resp_rc.content or b"")

                    dl_hash_rc = hashlib.sha256(content_rc).hexdigest() if content_rc else None
                    hash_match_rc = (dl_hash_rc == replace_hash_c) if (dl_hash_rc and replace_hash_c) else None
                    print(
                        {
                            "chunk_replace_source_size": replace_size_c,
                            "chunk_replace_download_size": len(content_rc),
                            "chunk_replace_size_match": len(content_rc) == replace_size_c,
                            "chunk_replace_source_sha256_prefix": replace_hash_c[:16] if replace_hash_c else None,
                            "chunk_replace_download_sha256_prefix": dl_hash_rc[:16] if dl_hash_rc else None,
                            "chunk_replace_hash_match": hash_match_rc,
                        }
                    )
                except Exception as ex:  # noqa: BLE001
                    print({"chunk_upload_failed": str(ex)})

            # --------------------------- Cleanup ---------------------------
            if cleanup_record and record_id:
                try:
                    print({"call": f"client.records.delete('{table_schema_name}', '{record_id}')"})
                    await backoff(lambda: client.records.delete(table_schema_name, record_id))
                    print({"record_deleted": True})
                except Exception as e:  # noqa: BLE001
                    print({"record_deleted": False, "error": str(e)})
            else:
                print({"record_deleted": False, "reason": "user opted to keep"})

            if cleanup_table:
                try:
                    print({"call": f"client.tables.delete('{TABLE_SCHEMA_NAME}')"})
                    await backoff(lambda: client.tables.delete(TABLE_SCHEMA_NAME))
                    print({"table_deleted": True})
                except Exception as e:  # noqa: BLE001
                    print({"table_deleted": False, "error": str(e)})
            else:
                print({"table_deleted": False, "reason": "user opted to keep"})
    finally:
        await credential.close()

    # Clean up generated test files
    for f in [generated_10mb, generated_8mb]:
        if f and f.exists():
            try:
                f.unlink()
                print({"test_file_deleted": True, "path": str(f)})
            except Exception as e:  # noqa: BLE001
                print({"test_file_deleted": False, "error": str(e)})

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
