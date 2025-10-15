import sys
from pathlib import Path
import os
import time
import traceback
from typing import Optional

# Add src to PYTHONPATH for local runs
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from dataverse_sdk import DataverseClient  # type: ignore
from azure.identity import InteractiveBrowserCredential  # type: ignore
import requests

entered = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
if not entered:
    print("No URL entered; exiting.")
    sys.exit(1)

base_url = entered.rstrip('/')
# Mode selection (numeric):
# 1 = small (single PATCH <128MB)
# 2 = chunk (streaming for any size)
# 3 = all (small + chunk)
mode_raw = input("Choose mode: 1) small  2) chunk  3) all [default 3]: ").strip()
if not mode_raw:
    mode_raw = '3'
if mode_raw not in {'1','2','3'}:
    print({"invalid_mode": mode_raw, "fallback": 3})
    mode_raw = '3'
mode_int = int(mode_raw)
run_small = mode_int in (1,3)
run_chunk = mode_int in (2,3)

delete_table_choice = input("Delete the table at end? (y/N): ").strip() or 'n'
cleanup_table = delete_table_choice.lower() in ("y", "yes", "true", "1")

delete_record_choice = input("Delete the created record at end? (Y/n): ").strip() or 'y'
cleanup_record = delete_record_choice.lower() in ("y", "yes", "true", "1")

credential = InteractiveBrowserCredential()
client = DataverseClient(base_url=base_url, credential=credential)

# --------------------------- Helpers ---------------------------

def log(call: str):
    print({"call": call})

# Simple SHA-256 helper with caching to avoid re-reading large files multiple times.
_FILE_HASH_CACHE = {}

def file_sha256(path: Path):  # returns (hex_digest, size_bytes)
    try:
        m = _FILE_HASH_CACHE.get(path)
        if m:
            return m[0], m[1]
        import hashlib  # noqa: WPS433
        h = hashlib.sha256()
        size = 0
        with path.open('rb') as f:  # stream to avoid high memory for large files
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                size += len(chunk)
                h.update(chunk)
        digest = h.hexdigest()
        _FILE_HASH_CACHE[path] = (digest, size)
        return digest, size
    except Exception:  # noqa: BLE001
        return None, None

def generate_test_pdf(size_mb: int = 10) -> Path:
    """Generate a dummy PDF file of specified size for testing purposes."""
    try:
        from reportlab.pdfgen import canvas  # type: ignore # noqa: WPS433
        from reportlab.lib.pagesizes import letter  # type: ignore # noqa: WPS433
    except ImportError:
        # Fallback: generate a simple binary file with PDF headers
        test_file = Path(__file__).resolve().parent / f"test_dummy_{size_mb}mb.pdf"
        target_size = size_mb * 1024 * 1024
        
        # Minimal PDF structure
        pdf_header = b"%PDF-1.4\n"
        pdf_body = b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        pdf_body += b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
        pdf_body += b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n"
        
        # Fill with dummy data to reach target size
        current_size = len(pdf_header) + len(pdf_body)
        padding_needed = target_size - current_size - 50  # Reserve space for trailer
        padding = b"% " + (b"padding " * (padding_needed // 8))[:padding_needed] + b"\n"
        
        pdf_trailer = b"xref\n0 4\ntrailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n0\n%%EOF\n"
        
        with test_file.open('wb') as f:
            f.write(pdf_header)
            f.write(pdf_body)
            f.write(padding)
            f.write(pdf_trailer)
        
        print({"test_pdf_generated": str(test_file), "size_mb": test_file.stat().st_size / (1024*1024)})
        return test_file
    
    # ReportLab available - generate proper PDF
    test_file = Path(__file__).resolve().parent / f"test_dummy_{size_mb}mb.pdf"
    c = canvas.Canvas(str(test_file), pagesize=letter)
    
    # Add pages with content until we reach target size
    target_size = size_mb * 1024 * 1024
    page_num = 0
    
    while test_file.exists() is False or test_file.stat().st_size < target_size:
        page_num += 1
        c.drawString(100, 750, f"Test PDF - Page {page_num}")
        c.drawString(100, 730, f"Generated for file upload testing")
        
        # Add some text to increase file size
        for i in range(50):
            c.drawString(50, 700 - (i * 12), f"Line {i}: " + "Sample text content " * 20)
        
        c.showPage()
        
        # Save periodically to check size
        if page_num % 10 == 0:
            c.save()
            if test_file.stat().st_size >= target_size:
                break
            c = canvas.Canvas(str(test_file), pagesize=letter)
    
    if not test_file.exists() or test_file.stat().st_size < target_size:
        c.save()
    
    print({"test_pdf_generated": str(test_file), "size_mb": test_file.stat().st_size / (1024*1024)})
    return test_file

def backoff(op, *, delays=(0,2,5,10), retry_status=(400,403,404,409,412,429,500,502,503,504)):
    last = None
    for d in delays:
        if d: time.sleep(d)
        try:
            return op()
        except Exception as ex:  # noqa: BLE001
            last = ex
            r = getattr(ex, 'response', None)
            code = getattr(r, 'status_code', None)
            if isinstance(ex, requests.exceptions.HTTPError) and code in retry_status:
                continue
            # For non-HTTP errors just retry the schedule
            continue
    if last:
        raise last

# --------------------------- Table ensure ---------------------------
TABLE_SCHEMA_NAME = "new_FileSample"
# If user wants new publisher prefix / naming, adjust above.

def ensure_table():
    # Check by schema
    existing = client.get_table_info(TABLE_SCHEMA_NAME)
    if existing:
        print({"table": TABLE_SCHEMA_NAME, "existed": True})
        return existing
    log("client.create_table('new_FileSample', schema={title})")
    info = client.create_table(TABLE_SCHEMA_NAME, {"title": "string"})
    print({"table": TABLE_SCHEMA_NAME, "existed": False, "metadata_id": info.get('metadata_id')})
    return info

try:
    table_info = ensure_table()
except Exception as e:  # noqa: BLE001
    print("Table ensure failed:")
    traceback.print_exc()
    sys.exit(1)

entity_set = table_info.get("entity_set_name")
logical = table_info.get("entity_logical_name") or entity_set.rstrip("s")
attr_prefix = logical.split('_',1)[0] if '_' in logical else logical
name_attr = f"{attr_prefix}_name"
small_file_attr_schema = f"{attr_prefix}_SmallDocument"  # second file attribute for small single-request demo
small_file_attr_logical = f"{attr_prefix}_smalldocument"  # expected logical name (lowercase)
chunk_file_attr_schema = f"{attr_prefix}_ChunkDocument"  # attribute for streaming chunk upload demo
chunk_file_attr_logical = f"{attr_prefix}_chunkdocument"  # expected logical name

def ensure_file_attribute_generic(schema_name: str, label: str, key_prefix: str):
    meta_id = table_info.get("metadata_id")
    if not meta_id:
        print({f"{key_prefix}_attribute": "skipped", "reason": "missing metadata_id"})
        return False
    odata = client._get_odata()
    # Probe existing
    try:
        url = (
            f"{odata.api}/EntityDefinitions({meta_id})/Attributes?$select=SchemaName&$filter="
            f"SchemaName eq '{schema_name}'"
        )
        r = odata._request("get", url, headers=odata._headers())
        r.raise_for_status()
        val = []
        try:
            val = r.json().get("value", [])
        except Exception:  # noqa: BLE001
            pass
        if any(a.get("SchemaName") == schema_name for a in val if isinstance(a, dict)):
            return True
    except Exception as ex:  # noqa: BLE001
        print({f"{key_prefix}_file_attr_probe_error": str(ex)})

    payload = {
        "@odata.type": "Microsoft.Dynamics.CRM.FileAttributeMetadata",
        "SchemaName": schema_name,
        "DisplayName": {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": label, "LanguageCode": int(client._config.language_code)}
            ],
        },
        "RequiredLevel": {"Value": "None"},
    }
    try:
        url = f"{odata.api}/EntityDefinitions({meta_id})/Attributes"
        r = odata._request("post", url, headers=odata._headers(), json=payload)
        r.raise_for_status()
        print({f"{key_prefix}_file_attribute_created": True})
        time.sleep(2)
        return True
    except Exception as ex:  # noqa: BLE001
        resp = getattr(ex, 'response', None)
        body_l = None
        try:
            body_l = resp.text.lower() if getattr(resp, 'text', None) else None
        except Exception:  # noqa: BLE001
            pass
        if body_l and ("duplicate" in body_l or "exists" in body_l):
            print({f"{key_prefix}_file_attribute_created": False, "reason": "already exists (race)"})
            return True
        print({f"{key_prefix}_file_attribute_created": False, "error": str(ex)})
        return False

# Conditionally ensure each attribute only if its mode is selected
if run_small:
    ensure_file_attribute_generic(small_file_attr_schema, "Small Document", "small")
if run_chunk:
    ensure_file_attribute_generic(chunk_file_attr_schema, "Chunk Document", "chunk")

# --------------------------- Record create ---------------------------
record_id = None
try:
    payload = {name_attr: "File Sample Record"}
    log(f"client.create('{entity_set}', payload)")
    rec = backoff(lambda: client.create(entity_set, payload))
    record_id = rec.get(f"{logical}id")
    print({"record_created": True, "id": record_id})
except Exception as e:  # noqa: BLE001
    print({"record_created": False, "error": str(e)})
    sys.exit(1)

if not record_id:
    print("No record id; aborting upload.")
    sys.exit(1)

src_hash_block = None

# --------------------------- Shared dataset helpers ---------------------------
_DATASET_INFO_CACHE = {}  # cache dict: file_path -> (path, size_bytes, sha256_hex)
_GENERATED_TEST_FILE = generate_test_pdf(10)  # track generated file for cleanup
_GENERATED_TEST_FILE_8MB = generate_test_pdf(8)  # track 8MB replacement file for cleanup

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
        backoff(lambda: client.upload_file(
            entity_set,
            record_id,
            small_file_attr_logical,
            str(DATASET_FILE),
            mode="small",
        ))
        print({"small_upload_completed": True, "small_source_size": small_file_size})
        odata = client._get_odata()
        dl_url_single = f"{odata.api}/{entity_set}({record_id})/{small_file_attr_logical}/$value"
        resp_single = odata._request("get", dl_url_single, headers=odata._headers())
        resp_single.raise_for_status()
        content_single = resp_single.content or b""
        import hashlib  # noqa: WPS433
        downloaded_hash = hashlib.sha256(content_single).hexdigest() if content_single else None
        hash_match = (downloaded_hash == src_hash) if (downloaded_hash and src_hash) else None
        print({
            "small_file_source_size": small_file_size,
            "small_file_download_size": len(content_single),
            "small_file_size_match": len(content_single) == small_file_size,
            "small_file_source_sha256_prefix": src_hash[:16] if src_hash else None,
            "small_file_download_sha256_prefix": downloaded_hash[:16] if downloaded_hash else None,
            "small_file_hash_match": hash_match,
        })
        
        # Now test replacing with an 8MB file
        print("Small single-request upload demo - REPLACE with 8MB file:")
        replacement_file, replace_size_small, replace_hash_small = get_dataset_info(_GENERATED_TEST_FILE_8MB)
        backoff(lambda: client.upload_file(
            entity_set,
            record_id,
            small_file_attr_logical,
            str(replacement_file),
            mode="small",
        ))
        print({"small_replace_upload_completed": True, "small_replace_source_size": replace_size_small})
        resp_single_replace = odata._request("get", dl_url_single, headers=odata._headers())
        resp_single_replace.raise_for_status()
        content_single_replace = resp_single_replace.content or b""
        downloaded_hash_replace = hashlib.sha256(content_single_replace).hexdigest() if content_single_replace else None
        hash_match_replace = (downloaded_hash_replace == replace_hash_small) if (downloaded_hash_replace and replace_hash_small) else None
        print({
            "small_replace_source_size": replace_size_small,
            "small_replace_download_size": len(content_single_replace),
            "small_replace_size_match": len(content_single_replace) == replace_size_small,
            "small_replace_source_sha256_prefix": replace_hash_small[:16] if replace_hash_small else None,
            "small_replace_download_sha256_prefix": downloaded_hash_replace[:16] if downloaded_hash_replace else None,
            "small_replace_hash_match": hash_match_replace,
        })
    except Exception as ex:  # noqa: BLE001
        print({"single_upload_failed": str(ex)})

# --------------------------- Chunk (streaming) upload demo ---------------------------
if run_chunk:
    print("Streaming chunk upload demo (upload_file_chunk):")
    try:
        DATASET_FILE, src_size_chunk, src_hash_chunk = get_dataset_info(_GENERATED_TEST_FILE)
        backoff(lambda: client.upload_file(
            entity_set,
            record_id,
            chunk_file_attr_logical,
            str(DATASET_FILE),
            mode="chunk",
        ))
        print({"chunk_upload_completed": True})
        odata = client._get_odata()
        dl_url_chunk = f"{odata.api}/{entity_set}({record_id})/{chunk_file_attr_logical}/$value"
        resp_chunk = odata._request("get", dl_url_chunk, headers=odata._headers())
        resp_chunk.raise_for_status()
        content_chunk = resp_chunk.content or b""
        import hashlib  # noqa: WPS433
        dst_hash_chunk = hashlib.sha256(content_chunk).hexdigest() if content_chunk else None
        hash_match_chunk = (dst_hash_chunk == src_hash_chunk) if (dst_hash_chunk and src_hash_chunk) else None
        print({
            "chunk_source_size": src_size_chunk,
            "chunk_download_size": len(content_chunk),
            "chunk_size_match": len(content_chunk) == src_size_chunk,
            "chunk_source_sha256_prefix": src_hash_chunk[:16] if src_hash_chunk else None,
            "chunk_download_sha256_prefix": dst_hash_chunk[:16] if dst_hash_chunk else None,
            "chunk_hash_match": hash_match_chunk,
        })
        
        # Now test replacing with an 8MB file
        print("Streaming chunk upload demo - REPLACE with 8MB file:")
        replacement_file, replace_size_chunk, replace_hash_chunk = get_dataset_info(_GENERATED_TEST_FILE_8MB)
        backoff(lambda: client.upload_file(
            entity_set,
            record_id,
            chunk_file_attr_logical,
            str(replacement_file),
            mode="chunk",
        ))
        print({"chunk_replace_upload_completed": True})
        resp_chunk_replace = odata._request("get", dl_url_chunk, headers=odata._headers())
        resp_chunk_replace.raise_for_status()
        content_chunk_replace = resp_chunk_replace.content or b""
        dst_hash_chunk_replace = hashlib.sha256(content_chunk_replace).hexdigest() if content_chunk_replace else None
        hash_match_chunk_replace = (dst_hash_chunk_replace == replace_hash_chunk) if (dst_hash_chunk_replace and replace_hash_chunk) else None
        print({
            "chunk_replace_source_size": replace_size_chunk,
            "chunk_replace_download_size": len(content_chunk_replace),
            "chunk_replace_size_match": len(content_chunk_replace) == replace_size_chunk,
            "chunk_replace_source_sha256_prefix": replace_hash_chunk[:16] if replace_hash_chunk else None,
            "chunk_replace_download_sha256_prefix": dst_hash_chunk_replace[:16] if dst_hash_chunk_replace else None,
            "chunk_replace_hash_match": hash_match_chunk_replace,
        })
    except Exception as ex:  # noqa: BLE001
        print({"chunk_upload_failed": str(ex)})

# --------------------------- Cleanup ---------------------------
if cleanup_record and record_id:
    try:
        log(f"client.delete('{entity_set}', '{record_id}')")
        backoff(lambda: client.delete(entity_set, record_id))
        print({"record_deleted": True})
    except Exception as e:  # noqa: BLE001
        print({"record_deleted": False, "error": str(e)})
else:
    print({"record_deleted": False, "reason": "user opted to keep"})

if cleanup_table:
    try:
        log(f"client.delete_table('{TABLE_SCHEMA_NAME}')")
        client.delete_table(TABLE_SCHEMA_NAME)
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
