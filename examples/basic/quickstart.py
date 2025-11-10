# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import sys
from pathlib import Path
import os
from typing import Optional

# Add src to PYTHONPATH for local runs
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from dataverse_sdk import DataverseClient
from dataverse_sdk.core.errors import MetadataError
from enum import IntEnum
from azure.identity import InteractiveBrowserCredential
import traceback
import requests
import time
from datetime import date, timedelta


entered = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
if not entered:
	print("No URL entered; exiting.")
	sys.exit(1)

base_url = entered.rstrip('/')
delete_choice = input("Delete the new_SampleItem table at end? (Y/n): ").strip() or "y"
delete_table_at_end = (str(delete_choice).lower() in ("y", "yes", "true", "1"))
# Ask once whether to pause between steps during this run
pause_choice = input("Pause between test steps? (y/N): ").strip() or "n"
pause_between_steps = (str(pause_choice).lower() in ("y", "yes", "true", "1"))
# Create a credential we can reuse (for DataverseClient)
credential = InteractiveBrowserCredential()
client = DataverseClient(base_url=base_url, credential=credential)

# Small helpers: call logging and step pauses
def log_call(call: str) -> None:
	print({"call": call})

def pause(next_step: str) -> None:
	if pause_between_steps:
		try:
			input(f"\nNext: {next_step} — press Enter to continue...")
		except EOFError:
			# If stdin is not available, just proceed
			pass

# Small generic backoff helper used only in this quickstart
# Include common transient statuses like 429/5xx to improve resilience.
def backoff_retry(op, *, delays=(0, 2, 5, 10, 20), retry_http_statuses=(400, 403, 404, 409, 412, 429, 500, 502, 503, 504), retry_if=None):
	last_exc = None
	for delay in delays:
		if delay:
			time.sleep(delay)
		try:
			return op()
		except Exception as ex:
			print(f'Request failed: {ex}')
			last_exc = ex
			if retry_if and retry_if(ex):
				print("Retrying operation...")
				continue
			if isinstance(ex, requests.exceptions.HTTPError):
				code = getattr(getattr(ex, 'response', None), 'status_code', None)
				if code in retry_http_statuses:
					print("Retrying operation...")
					continue
			break
	if last_exc:
		raise last_exc
	
# Enum demonstrating local option set creation with multilingual labels (for French labels to work, enable French language in the environment first)
class Status(IntEnum):
	Active = 1
	Inactive = 2
	Archived = 5
	__labels__ = {
		1033: {
			"Active": "Active",
			"Inactive": "Inactive",
			"Archived": "Archived",
		},
		1036: {
			"Active": "Actif",
			"Inactive": "Inactif",
			"Archived": "Archivé",
		}
	}

print("Ensure custom table exists (Metadata):")
table_info = None
created_this_run = False

# Check for existing table using list_tables
log_call("client.list_tables()")
tables = client.list_tables()
existing_table = next((t for t in tables if t.get("SchemaName") == "new_SampleItem"), None)
if existing_table:
	table_info = client.get_table_info("new_SampleItem")
	created_this_run = False
	print({
		"table": table_info.get("entity_schema"),
		"existed": True,
		"entity_set": table_info.get("entity_set_name"),
		"logical": table_info.get("entity_logical_name"),
		"metadata_id": table_info.get("metadata_id"),
	})

else:
	# Create it since it doesn't exist
	try:
		log_call("client.create_table('new_SampleItem', schema={code,count,amount,when,active,status<enum>})")
		table_info = client.create_table(
			"new_SampleItem",
			{
				"code": "string",
				"count": "int",
				"amount": "decimal",
				"when": "datetime",
				"active": "bool",
				"status": Status,
			},
		)
		created_this_run = True if table_info and table_info.get("columns_created") else False
		print({
			"table": table_info.get("entity_schema") if table_info else None,
			"existed": False,
			"entity_set": table_info.get("entity_set_name") if table_info else None,
			"logical": table_info.get("entity_logical_name") if table_info else None,
			"metadata_id": table_info.get("metadata_id") if table_info else None,
		})
	except Exception as e:
		# Print full stack trace and any HTTP response details if present
		print("Create table failed:")
		traceback.print_exc()
		resp = getattr(e, 'response', None)
		if resp is not None:
			try:
				print({
					"status": resp.status_code,
					"url": getattr(resp, 'url', None),
					"body": resp.text[:2000] if getattr(resp, 'text', None) else None,
				})
			except Exception:
				pass
		# Fail fast: all operations must use the custom table
		sys.exit(1)
entity_schema = table_info.get("entity_schema") or "new_SampleItem"
logical = table_info.get("entity_logical_name")
metadata_id = table_info.get("metadata_id")
if not metadata_id:
	refreshed_info = client.get_table_info(entity_schema) or {}
	metadata_id = refreshed_info.get("metadata_id")
	if metadata_id:
		table_info["metadata_id"] = metadata_id

# Derive attribute logical name prefix from the entity logical name (segment before first underscore)
attr_prefix = logical.split("_", 1)[0] if "_" in logical else logical
code_key = f"{attr_prefix}_code"
count_key = f"{attr_prefix}_count"
amount_key = f"{attr_prefix}_amount"
when_key = f"{attr_prefix}_when"
status_key = f"{attr_prefix}_status"
id_key = f"{logical}id"

def summary_from_record(rec: dict) -> dict:
	return {
		"code": rec.get(code_key),
		"count": rec.get(count_key),
		"amount": rec.get(amount_key),
		"when": rec.get(when_key),
	}

def print_line_summaries(label: str, summaries: list[dict]) -> None:
	print(label)
	for s in summaries:
		print(
			f" - id={s.get('id')} code={s.get('code')} "
			f"count={s.get('count')} amount={s.get('amount')} when={s.get('when')}"
		)

def _has_installed_language(base_url: str, credential, lcid: int) -> bool:
	try:
		token = credential.get_token(f"{base_url}/.default").token
		url = f"{base_url}/api/data/v9.2/RetrieveAvailableLanguages()"
		headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
		resp = requests.get(url, headers=headers, timeout=15)
		if not resp.ok:
			return False
		data = resp.json() if resp.content else {}
		langs: list[int] = []
		for val in data.values():
			if isinstance(val, list) and val and all(isinstance(x, int) for x in val):
				langs = val
				break
		print({"lang_check": {"endpoint": url, "status": resp.status_code, "found": langs, "using": lcid in langs}})
		return lcid in langs
	except Exception:
		return False

# if French language (1036) is installed, we use labels in both English and French
use_french_labels = _has_installed_language(base_url, credential, 1036)
if use_french_labels:
	print({"labels_language": "fr", "note": "French labels in use."})
else:
	print({"labels_language": "en", "note": "Using English (and numeric values)."})

# 2) Create a record in the new table
print("Create records (OData) demonstrating single create and bound CreateMultiple (multi):")

# Define base payloads
single_payload = {
	f"{attr_prefix}_name": "Sample A",
	code_key: "X001",
	count_key: 42,
	amount_key: 123.45,
	when_key: "2025-01-01",
	f"{attr_prefix}_active": True,
	status_key: ("Actif" if use_french_labels else Status.Active.value),
}
# Generate multiple payloads
# Distribution update: roughly one-third English labels, one-third French labels, one-third raw integer values.
# We cycle per record: index % 3 == 1 -> English label, == 2 -> French label (if available, else English), == 0 -> integer value.
multi_payloads: list[dict] = []
base_date = date(2025, 1, 2)
# Fixed 6-step cycle pattern encapsulated in helper: Active, Inactive, Actif, Inactif, 1, 2 (repeat)
def _status_value_for_index(idx: int, use_french: bool):
	pattern = [
		("label", "Active"),
		("label", "Inactive"),
		("fr_label", "Actif"),
		("fr_label", "Inactif"),
		("int", Status.Active.value),
		("int", Status.Inactive.value),
	]
	kind, raw = pattern[(idx - 1) % len(pattern)]
	if kind == "label":
		return raw
	if kind == "fr_label":
		if use_french:
			return raw
		return "Active" if raw == "Actif" else "Inactive"
	return raw

for i in range(1, 16):
	multi_payloads.append({
		f"{attr_prefix}_name": f"Sample {i:02d}",
		code_key: f"X{200 + i:03d}",
		count_key: 5 * i,
		amount_key: round(10.0 * i, 2),
		when_key: (base_date + timedelta(days=i - 1)).isoformat(),
		f"{attr_prefix}_active": True,
		status_key: _status_value_for_index(i, use_french_labels),
	})

record_ids: list[str] = []

try:
	# Single create returns list[str] (length 1)
	log_call(f"client.create('{logical}', single_payload)")
	single_ids = backoff_retry(lambda: client.create(logical, single_payload))
	if not (isinstance(single_ids, list) and len(single_ids) == 1):
		raise RuntimeError("Unexpected single create return shape (expected one-element list)")
	record_ids.extend(single_ids)

	# Multi create returns list[str]
	log_call(f"client.create('{logical}', multi_payloads)")
	multi_ids = backoff_retry(lambda: client.create(logical, multi_payloads))
	if isinstance(multi_ids, list):
		record_ids.extend([mid for mid in multi_ids if isinstance(mid, str)])
	else:
		print({"multi_unexpected_type": type(multi_ids).__name__, "value_preview": str(multi_ids)[:300]})

	print({"entity": logical, "created_ids": record_ids})
	print_line_summaries("Created record summaries (IDs only; representation not fetched):", [{"id": rid} for rid in record_ids[:1]])
except Exception as e:
	# Surface detailed info for debugging (especially multi-create failures)
	print(f"Create failed: {e}")
	resp = getattr(e, 'response', None)
	if resp is not None:
		try:
			print({
				'status': resp.status_code,
				'url': getattr(resp, 'url', None),
				'body': resp.text[:2000] if getattr(resp, 'text', None) else None,
				'headers': {k: v for k, v in getattr(resp, 'headers', {}).items() if k.lower() in ('request-id','activityid','dataverse-instanceversion','content-type')}
			})
		except Exception:
			pass
	sys.exit(1)

pause("Next: Read record")

# 3) Read record via OData
print("Read (OData):")
try:
	if record_ids:
		# Read only the first record and move on
		target = record_ids[0]
		log_call(f"client.get('{logical}', '{target}')")
		rec = backoff_retry(lambda: client.get(logical, target))
		print_line_summaries("Read record summary:", [{"id": target, **summary_from_record(rec)}])
	else:
		raise RuntimeError("No record created; skipping read.")
except Exception as e:
	print(f"Get failed: {e}")
# 3.5) Update record, then read again and verify
print("Update (OData) and verify:")
# Show what will be updated and planned update calls, then pause
try:
	if not record_ids:
		raise RuntimeError("No record created; skipping update.")

	update_data = {
		f"{attr_prefix}_code": "X002",
		f"{attr_prefix}_count": 99,
		f"{attr_prefix}_amount": 543.21,
		f"{attr_prefix}_when": "2025-02-02",
		f"{attr_prefix}_active": False,
		status_key: ("Inactif" if use_french_labels else Status.Inactive.value),
	}
	expected_checks = {
		f"{attr_prefix}_code": "X002",
		f"{attr_prefix}_count": 99,
		f"{attr_prefix}_active": False,
		status_key: Status.Inactive.value,
	}
	amount_key = f"{attr_prefix}_amount"

	# Describe what is changing
	print(
		{
			"updating_to": {
				code_key: update_data[code_key],
				count_key: update_data[count_key],
				amount_key: update_data[amount_key],
				when_key: update_data[when_key],
			}
		}
	)

	# Choose a single target to update to keep other records different
	target_id = record_ids[0]
	pause("Execute Update")

	# Update only the chosen record and summarize
	log_call(f"client.update('{logical}', '{target_id}', update_data)")
	# Perform update (returns None); follow-up read to verify
	backoff_retry(lambda: client.update(logical, target_id, update_data))
	verify_rec = backoff_retry(lambda: client.get(logical, target_id))
	for k, v in expected_checks.items():
		assert verify_rec.get(k) == v, f"Field {k} expected {v}, got {verify_rec.get(k)}"
	got = verify_rec.get(amount_key)
	got_f = float(got) if got is not None else None
	assert got_f is not None and abs(got_f - 543.21) < 1e-6, f"Field {amount_key} expected 543.21, got {got}"
	print({"entity": logical, "updated": True})
	print_line_summaries("Updated record summary:", [{"id": target_id, **summary_from_record(verify_rec)}])
except Exception as e:
	print(f"Update/verify failed: {e}")
	sys.exit(1)

# 3.6) Bulk update (UpdateMultiple) demo: update count field on up to first 5 remaining records
print("Bulk update (UpdateMultiple) demo:")
try:
	if len(record_ids) > 1:
		# Prepare a small subset to update (skip the first already updated one)
		subset = record_ids[1:6]
		bulk_updates = []
		for idx, rid in enumerate(subset, start=1):
			# Simple deterministic changes so user can observe
			bulk_updates.append({
				id_key: rid,
				count_key: 100 + idx,  # new count values
			})
		log_call(f"client.update('{logical}', <{len(bulk_updates)} ids>, <patches>)")
		# Unified update handles multiple via list of patches (returns None)
		backoff_retry(lambda: client.update(logical, subset, bulk_updates))
		print({"bulk_update_requested": len(bulk_updates), "bulk_update_completed": True})
		# Verify the updated count values by refetching the subset
		verification = []
		# Small delay to reduce risk of any brief replication delay
		time.sleep(1)
		for rid in subset:
			rec = backoff_retry(lambda rid=rid: client.get(logical, rid))
			verification.append({
				"id": rid,
				"count": rec.get(count_key),
			})
		print({"bulk_update_verification": verification})
	else:
		print({"bulk_update_skipped": True, "reason": "not enough records"})
except Exception as e:
	print(f"Bulk update failed: {e}")

# 4) Query records via SQL (?sql parameter))
print("Query (SQL via ?sql query parameter):")
try:
	import time
	pause("Execute SQL Query")

	def _run_query():
		cols = f"{id_key}, {code_key}, {amount_key}, {when_key}"
		query = f"SELECT TOP 2 {cols} FROM {logical} ORDER BY {attr_prefix}_amount DESC"
		log_call(f"client.query_sql(\"{query}\") (Web API ?sql=)")
		return client.query_sql(query)

	def _retry_if(ex: Exception) -> bool:
		msg = str(ex) if ex else ""
		return ("Invalid table name" in msg) or ("Invalid object name" in msg)

	rows = backoff_retry(_run_query, delays=(0, 2, 5), retry_http_statuses=(), retry_if=_retry_if)
	id_key = f"{logical}id"
	ids = [r.get(id_key) for r in rows if isinstance(r, dict) and r.get(id_key)]
	print({"entity": logical, "rows": len(rows) if isinstance(rows, list) else 0, "ids": ids})
	record_summaries = []
	for row in rows if isinstance(rows, list) else []:
		record_summaries.append(
			{
				"id": row.get(id_key),
				"code": row.get(code_key),
				"count": row.get(count_key),
				"amount": row.get(amount_key),
				"when": row.get(when_key),
			}
		)
	print_line_summaries("SQL record summaries (top 2 by amount):", record_summaries)
except Exception as e:
	print(f"SQL query failed: {e}")

# Pause between SQL query and retrieve-multiple demos
pause("Retrieve multiple (OData paging demos)")

# 4.5) Retrieve multiple via OData paging (scenarios)
def run_paging_demo(label: str, *, top: Optional[int], page_size: Optional[int]) -> None:
	print("")
	print({"paging_demo": label, "top": top, "page_size": page_size})
	total = 0
	page_index = 0
	_select = [id_key, code_key, amount_key, when_key, status_key]
	_orderby = [f"{code_key} asc"]
	for page in client.get(
		logical,
		select=_select,
		filter=None,
		orderby=_orderby,
		top=top,
		expand=None,
		page_size=page_size,
	):
		page_index += 1
		total += len(page)
		print({
			"page": page_index,
			"page_size": len(page),
			"sample": [
				{
					"id": r.get(id_key),
					"code": r.get(code_key),
					"amount": r.get(amount_key),
					"when": r.get(when_key),
					"status": r.get(status_key),
				}
				for r in page[:5]
			],
		})
	print({"paging_demo_done": label, "pages": page_index, "total_rows": total})
	print("")

print("")
print("==============================")
print("Retrieve multiple (OData paging demos)")
print("==============================")
try:
	# 1) Tiny page size, no top: force multiple pages
	run_paging_demo("page_size=2 (no top)", top=None, page_size=2)
	pause("Next paging demo: top=3, page_size=2")

	# 2) Limit total results while keeping small pages
	run_paging_demo("top=3, page_size=2", top=3, page_size=2)
	pause("Next paging demo: top=2 (default page size)")

	# 3) Limit total results with default server page size (likely one page)
	run_paging_demo("top=2 (default page size)", top=2, page_size=None)
except Exception as e:
	print(f"Retrieve multiple demos failed: {e}")
# 5) Delete record
print("Delete (OData):")
# Show deletes to be executed (single + bulk)
if 'record_ids' in locals() and record_ids:
	print({"delete_count": len(record_ids)})
pause("Execute Delete (single then bulk)")
try:
	if record_ids:
		single_target = record_ids[0]
		rest_targets = record_ids[1:]
		single_error: Optional[str] = None
		bulk_job_id: Optional[str] = None
		bulk_error: Optional[str] = None

		try:
			log_call(f"client.delete('{logical}', '{single_target}')")
			backoff_retry(lambda: client.delete(logical, single_target))
		except Exception as ex:
			single_error = str(ex)

		half = max(1, len(rest_targets) // 2)
		bulk_targets = rest_targets[:half]
		sequential_targets = rest_targets[half:]
		bulk_error = None
		sequential_error = None

		# Fire-and-forget bulk delete for the first portion
		try:
			log_call(f"client.delete('{logical}', <{len(bulk_targets)} ids>, use_bulk_delete=True)")
			bulk_job_id = client.delete(logical, bulk_targets)
		except Exception as ex:
			bulk_error = str(ex)

		# Sequential deletes for the remainder
		try:
			log_call(f"client.delete('{logical}', <{len(sequential_targets)} ids>, use_bulk_delete=False)")
			for rid in sequential_targets:
				backoff_retry(lambda rid=rid: client.delete(logical, rid, use_bulk_delete=False))
		except Exception as ex:
			sequential_error = str(ex)

		print({
			"entity": logical,
			"delete_single": {
				"id": single_target,
				"error": single_error,
			},
			"delete_bulk": {
				"count": len(bulk_targets),
				"job_id": bulk_job_id,
				"error": bulk_error,
			},
			"delete_sequential": {
				"count": len(sequential_targets),
				"error": sequential_error,
			},
		})
	else:
		raise RuntimeError("No record created; skipping delete.")
except Exception as e:
	print(f"Delete failed: {e}")

pause("Next: column metadata helpers")

# 6) Column metadata helpers: column create/delete
print("Column metadata helpers (create/delete column):")
scratch_column = f"scratch_{int(time.time())}"
column_payload = {scratch_column: "string"}
try:
	log_call(f"client.create_column('{entity_schema}', {repr(column_payload)})")
	column_create = client.create_columns(entity_schema, column_payload)
	if not isinstance(column_create, list) or not column_create:
		raise RuntimeError("create_column did not return schema list")
	created_details = column_create
	if not all(isinstance(item, str) for item in created_details):
		raise RuntimeError("create_column entries were not schema strings")
	attribute_schema = created_details[0]
	odata_client = client._get_odata()
	exists_after_create = None
	exists_after_delete = None
	attr_type_before = None
	if metadata_id and attribute_schema:
		_ready_message = "Column metadata not yet available"
		def _metadata_after_create():
			meta = odata_client._get_attribute_metadata(
				metadata_id,
				attribute_schema,
				extra_select="@odata.type,AttributeType",
			)
			if not meta or not meta.get("MetadataId"):
				raise RuntimeError(_ready_message)
			return meta

		ready_meta = backoff_retry(
			_metadata_after_create,
			delays=(0, 1, 2, 4, 8),
			retry_http_statuses=(),
			retry_if=lambda exc: isinstance(exc, RuntimeError) and str(exc) == _ready_message,
		)
		exists_after_create = bool(ready_meta)
		raw_type = ready_meta.get("@odata.type") or ready_meta.get("AttributeType")
		if isinstance(raw_type, str):
			attr_type_before = raw_type
			lowered = raw_type.lower()
	delete_target = attribute_schema or scratch_column
	log_call(f"client.delete_column('{entity_schema}', '{delete_target}')")

	def _delete_column():
		return client.delete_columns(entity_schema, delete_target)

	column_delete = backoff_retry(
		_delete_column,
		delays=(0, 1, 2, 4, 8),
		retry_http_statuses=(),
		retry_if=lambda exc: (
			isinstance(exc, MetadataError)
			or "not found" in str(exc).lower()
			or "not yet available" in str(exc).lower()
		),
	)
	if not isinstance(column_delete, list) or not column_delete:
		raise RuntimeError("delete_column did not return schema list")
	deleted_details = column_delete
	if not all(isinstance(item, str) for item in deleted_details):
		raise RuntimeError("delete_column entries were not schema strings")
	if attribute_schema not in deleted_details:
		raise RuntimeError("delete_column response missing expected schema name")
	if metadata_id and attribute_schema:
		_delete_message = "Column metadata still present after delete"
		def _ensure_removed():
			meta = odata_client._get_attribute_metadata(metadata_id, attribute_schema)
			if meta:
				raise RuntimeError(_delete_message)
			return True

		removed = backoff_retry(
			_ensure_removed,
			delays=(0, 1, 2, 4, 8),
			retry_http_statuses=(),
			retry_if=lambda exc: isinstance(exc, RuntimeError) and str(exc) == _delete_message,
		)
		exists_after_delete = not removed
	print({
		"created_column": scratch_column,
		"create_summary": created_details,
		"delete_summary": deleted_details,
		"attribute_type_before_delete": attr_type_before,
		"exists_after_create": exists_after_create,
		"exists_after_delete": exists_after_delete,
	})
except MetadataError as meta_err:
	print({"column_metadata_error": str(meta_err)})
except Exception as exc:
	print({"column_metadata_unexpected": str(exc)})

pause("Next: Cleanup table")

# 7) Cleanup: delete the custom table if it exists
print("Cleanup (Metadata):")
if delete_table_at_end:
	try:
		log_call("client.get_table_info('new_SampleItem')")
		info = client.get_table_info("new_SampleItem")
		if info:
			log_call("client.delete_table('new_SampleItem')")
			client.delete_table("new_SampleItem")
			print({"table_deleted": True})
		else:
			print({"table_deleted": False, "reason": "not found"})
	except Exception as e:
		print(f"Delete table failed: {e}")
else:
	print({"table_deleted": False, "reason": "user opted to keep table"})
