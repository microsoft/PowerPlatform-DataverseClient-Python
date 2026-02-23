# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - File Upload Example

This example demonstrates file upload capabilities using the
PowerPlatform-Dataverse-Client SDK with automatic chunking for large files.

Prerequisites:
    pip install PowerPlatform-Dataverse-Client
    pip install azure-identity

For local development, you can also run from source by uncommenting the sys.path line below.
"""

import sys
from pathlib import Path
import os
import time
import traceback
from typing import Optional

# Uncomment for local development from source
# sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from PowerPlatform.Dataverse.client import DataverseClient
from azure.identity import InteractiveBrowserCredential  # type: ignore
import requests

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

credential = InteractiveBrowserCredential()
client = DataverseClient(base_url=base_url, credential=credential)

# --------------------------- Helpers ---------------------------


def log(call: str):
    print({"call": call})


# Simple SHA-256 helper with caching to avoid re-reading large files multiple times.
_FILE_HASH_CACHE = {}
ATTRIBUTE_VISIBILITY_DELAYS = (0, 3, 10, 20, 35, 50, 70, 90, 120)


def file_sha256(path: Path):  # returns (hex_digest, size_bytes)
    try:
        m = _FILE_HASH_CACHE.get(path)
        if m:
            return m[0], m[1]
        import hashlib  # noqa: WPS433

        h = hashlib.sha256()
        size = 0
        with path.open("rb") as f:  # stream to avoid high memory for large files
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                size += len(chunk)
                h.update(chunk)
        digest = h.hexdigest()
        _FILE_HASH_CACHE[path] = (digest, size)
        return digest, size
    except Exception:  # noqa: BLE001
        return None, None


def generate_test_file(size_mb: int = 10) -> Path:
    """Generate a dummy text file of specified size for testing purposes.

    Creates a plain text file with repeating content to reach the target
    size. No external dependencies required.
    """
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


def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
    last = None
    total_delay = 0
    attempts = 0
    for d in delays:
        if d:
            time.sleep(d)
            total_delay += d
        attempts += 1
        try:
            result = op()
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


# --------------------------- Table ensure ---------------------------
TABLE_SCHEMA_NAME = "new_FileSample"


def ensure_table():
    # Check by schema
    existing = backoff(lambda: client.tables.get(TABLE_SCHEMA_NAME))
    if existing:
        print({"table": TABLE_SCHEMA_NAME, "existed": True})
        return existing
    log(f"client.tables.create('{TABLE_SCHEMA_NAME}', schema={{'new_Title': 'string'}})")
    info = backoff(lambda: client.tables.create(TABLE_SCHEMA_NAME, {"new_Title": "string"}))
    print({"table": TABLE_SCHEMA_NAME, "existed": False, "metadata_id": info.get("metadata_id")})
    return info


try:
    table_info = ensure_table()
except Exception as e:  # noqa: BLE001
    print("Table ensure failed:")
    traceback.print_exc()
    sys.exit(1)

entity_set = table_info.get("entity_set_name")
table_schema_name = table_info.get("table_schema_name")
attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
name_attr = f"{attr_prefix}_name"
small_file_attr_schema = f"{attr_prefix}_SmallDocument"  # second file attribute for small single-request demo
chunk_file_attr_schema = f"{attr_prefix}_ChunkDocument"  # attribute for streaming chunk upload demo

# --------------------------- Record create ---------------------------
record_id = None
try:
    payload = {name_attr: "File Sample Record"}
    log(f"client.records.create('{table_schema_name}', payload)")
    record_id = backoff(lambda: client.records.create(table_schema_name, payload))
    print({"record_created": True, "id": record_id, "table schema name": table_schema_name})
except Exception as e:  # noqa: BLE001
    print({"record_created": False, "error": str(e)})
    sys.exit(1)

if not record_id:
    print("No record id; aborting upload.")
    sys.exit(1)

src_hash_block = None

# --------------------------- Shared dataset helpers ---------------------------
_DATASET_INFO_CACHE = {}  # cache dict: file_path -> (path, size_bytes, sha256_hex)
_GENERATED_TEST_FILE = generate_test_file(10)  # track generated file for cleanup
_GENERATED_TEST_FILE_8MB = generate_test_file(8)  # track 8MB replacement file for cleanup


def get_dataset_info(file_path: Path):
    if file_path in _DATASET_INFO_CACHE:
        return _DATASET_INFO_CACHE[file_path]

    sha_hex, size = file_sha256(file_path)
    info = (file_path, size, sha_hex)
    _DATASET_INFO_CACHE[file_path] = info
    return info


# --------------------------- Small single-request file upload demo ---------------------------
if run_small:
    print("Small single-request upload demo:")
    try:
        DATASET_FILE, small_file_size, src_hash = get_dataset_info(_GENERATED_TEST_FILE)
        backoff(
            lambda: client.files.upload(
                table=table_schema_name,
                record_id=record_id,
                file_column=small_file_attr_schema,
                path=str(DATASET_FILE),
                mode="small",
            )
        )
        print({"small_upload_completed": True, "small_source_size": small_file_size})
        odata = client._get_odata()
        dl_url_single = (
            f"{odata.api}/{entity_set}({record_id})/{small_file_attr_schema.lower()}/$value"  # raw entity_set URL OK
        )
        resp_single = backoff(lambda: odata._request("get", dl_url_single))
        content_single = resp_single.content or b""
        import hashlib  # noqa: WPS433

        downloaded_hash = hashlib.sha256(content_single).hexdigest() if content_single else None
        hash_match = (downloaded_hash == src_hash) if (downloaded_hash and src_hash) else None
        print(
            {
                "small_file_source_size": small_file_size,
                "small_file_download_size": len(content_single),
                "small_file_size_match": len(content_single) == small_file_size,
                "small_file_source_sha256_prefix": src_hash[:16] if src_hash else None,
                "small_file_download_sha256_prefix": downloaded_hash[:16] if downloaded_hash else None,
                "small_file_hash_match": hash_match,
            }
        )

        # Now test replacing with an 8MB file
        print("Small single-request upload demo - REPLACE with 8MB file:")
        replacement_file, replace_size_small, replace_hash_small = get_dataset_info(_GENERATED_TEST_FILE_8MB)
        backoff(
            lambda: client.files.upload(
                table=table_schema_name,
                record_id=record_id,
                file_column=small_file_attr_schema,
                path=str(replacement_file),
                mode="small",
                if_none_match=False,
            )
        )
        print({"small_replace_upload_completed": True, "small_replace_source_size": replace_size_small})
        resp_single_replace = backoff(lambda: odata._request("get", dl_url_single))
        content_single_replace = resp_single_replace.content or b""
        downloaded_hash_replace = hashlib.sha256(content_single_replace).hexdigest() if content_single_replace else None
        hash_match_replace = (
            (downloaded_hash_replace == replace_hash_small)
            if (downloaded_hash_replace and replace_hash_small)
            else None
        )
        print(
            {
                "small_replace_source_size": replace_size_small,
                "small_replace_download_size": len(content_single_replace),
                "small_replace_size_match": len(content_single_replace) == replace_size_small,
                "small_replace_source_sha256_prefix": replace_hash_small[:16] if replace_hash_small else None,
                "small_replace_download_sha256_prefix": (
                    downloaded_hash_replace[:16] if downloaded_hash_replace else None
                ),
                "small_replace_hash_match": hash_match_replace,
            }
        )
    except Exception as ex:  # noqa: BLE001
        print({"single_upload_failed": str(ex)})

# --------------------------- Chunk (streaming) upload demo ---------------------------
if run_chunk:
    print("Streaming chunk upload demo (mode='chunk'):")
    try:
        DATASET_FILE, src_size_chunk, src_hash_chunk = get_dataset_info(_GENERATED_TEST_FILE)
        backoff(
            lambda: client.files.upload(
                table=table_schema_name,
                record_id=record_id,
                file_column=chunk_file_attr_schema,
                path=str(DATASET_FILE),
                mode="chunk",
            )
        )
        print({"chunk_upload_completed": True})
        odata = client._get_odata()
        dl_url_chunk = f"{odata.api}/{entity_set}({record_id})/{chunk_file_attr_schema.lower()}/$value"  # raw entity_set for download
        resp_chunk = backoff(lambda: odata._request("get", dl_url_chunk))
        content_chunk = resp_chunk.content or b""
        import hashlib  # noqa: WPS433

        dst_hash_chunk = hashlib.sha256(content_chunk).hexdigest() if content_chunk else None
        hash_match_chunk = (dst_hash_chunk == src_hash_chunk) if (dst_hash_chunk and src_hash_chunk) else None
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
        # Now test replacing with an 8MB file
        print("Streaming chunk upload demo - REPLACE with 8MB file:")
        replacement_file, replace_size_chunk, replace_hash_chunk = get_dataset_info(_GENERATED_TEST_FILE_8MB)
        backoff(
            lambda: client.files.upload(
                table=table_schema_name,
                record_id=record_id,
                file_column=chunk_file_attr_schema,
                path=str(replacement_file),
                mode="chunk",
                if_none_match=False,
            )
        )
        print({"chunk_replace_upload_completed": True})
        resp_chunk_replace = backoff(lambda: odata._request("get", dl_url_chunk))
        content_chunk_replace = resp_chunk_replace.content or b""
        dst_hash_chunk_replace = hashlib.sha256(content_chunk_replace).hexdigest() if content_chunk_replace else None
        hash_match_chunk_replace = (
            (dst_hash_chunk_replace == replace_hash_chunk) if (dst_hash_chunk_replace and replace_hash_chunk) else None
        )
        print(
            {
                "chunk_replace_source_size": replace_size_chunk,
                "chunk_replace_download_size": len(content_chunk_replace),
                "chunk_replace_size_match": len(content_chunk_replace) == replace_size_chunk,
                "chunk_replace_source_sha256_prefix": replace_hash_chunk[:16] if replace_hash_chunk else None,
                "chunk_replace_download_sha256_prefix": dst_hash_chunk_replace[:16] if dst_hash_chunk_replace else None,
                "chunk_replace_hash_match": hash_match_chunk_replace,
            }
        )
    except Exception as ex:  # noqa: BLE001
        print({"chunk_upload_failed": str(ex)})

# --------------------------- Cleanup ---------------------------
if cleanup_record and record_id:
    try:
        log(f"client.records.delete('{table_schema_name}', '{record_id}')")
        backoff(lambda: client.records.delete(table_schema_name, record_id))
        print({"record_deleted": True})
    except Exception as e:  # noqa: BLE001
        print({"record_deleted": False, "error": str(e)})
else:
    print({"record_deleted": False, "reason": "user opted to keep"})

if cleanup_table:
    try:
        log(f"client.tables.delete('{TABLE_SCHEMA_NAME}')")
        backoff(lambda: client.tables.delete(TABLE_SCHEMA_NAME))
        print({"table_deleted": True})
    except Exception as e:  # noqa: BLE001
        print({"table_deleted": False, "error": str(e)})
else:
    print({"table_deleted": False, "reason": "user opted to keep"})

# Clean up generated test file if it was created
if _GENERATED_TEST_FILE and _GENERATED_TEST_FILE.exists():
    try:
        _GENERATED_TEST_FILE.unlink()
        print({"test_file_deleted": True, "path": str(_GENERATED_TEST_FILE)})
    except Exception as e:  # noqa: BLE001
        print({"test_file_deleted": False, "error": str(e)})

# Clean up 8MB replacement test file if it was created
if _GENERATED_TEST_FILE_8MB and _GENERATED_TEST_FILE_8MB.exists():
    try:
        _GENERATED_TEST_FILE_8MB.unlink()
        print({"test_file_8mb_deleted": True, "path": str(_GENERATED_TEST_FILE_8MB)})
    except Exception as e:  # noqa: BLE001
        print({"test_file_8mb_deleted": False, "error": str(e)})

print("Done.")
