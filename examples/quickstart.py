import sys
from pathlib import Path
import os
from typing import Optional

# Add src to PYTHONPATH for local runs
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from dataverse_sdk import DataverseClient
from azure.identity import InteractiveBrowserCredential
import traceback
import requests
import time
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


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
instant_create_choice = input("Run instant create demo? (y/N): ").strip() or "n"
run_instant_create = (str(instant_create_choice).lower() in ("y", "yes", "true", "1"))
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

# Helper: delete a table if it exists
def delete_table_if_exists(table_name: str) -> None:
	try:
		log_call(f"client.get_table_info('{table_name}')")
		info = client.get_table_info(table_name)
		if info:
			log_call(f"client.delete_table('{table_name}')")
			client.delete_table(table_name)
			print({"table_deleted": True})
		else:
			print({"table_deleted": False, "reason": "not found"})
	except Exception as e:
		print({f"Delete table failed": str(e)})

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
				continue
			if isinstance(ex, requests.exceptions.HTTPError):
				code = getattr(getattr(ex, 'response', None), 'status_code', None)
				if code in retry_http_statuses:
					continue
			break
	if last_exc:
		raise last_exc

print("Ensure custom table exists (Metadata):")
table_info = None
created_this_run = False

# Timing metrics for comparison (seconds)
instant_create_seconds: float | None = None
standard_create_seconds: float | None = None
warm_up_seconds: float | None = None

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
		log_call("client.create_table('new_SampleItem', schema={code,count,amount,when,active})")
		_t0_standard = time.perf_counter()
		table_info = client.create_table(
			"new_SampleItem",
			{
				"code": "string",
				"count": "int",
				"amount": "decimal",
				"when": "datetime",
				"active": "bool",
			},
		)
		standard_create_seconds = time.perf_counter() - _t0_standard
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
entity_set = table_info.get("entity_set_name")
logical = table_info.get("entity_logical_name") or entity_set.rstrip("s")

if run_instant_create:
	# call early to warm up for instant create
	log_call("client.warm_up_instant_create()")
	try:
		_t0_warm = time.perf_counter()
		client.warm_up_instant_create()
		warm_up_seconds = time.perf_counter() - _t0_warm
		print({"warm_up_for_instant_create": True, "warm_up_seconds": warm_up_seconds})
	except Exception as warm_ex:
		print({"warm_up_for_instant_create_error": str(warm_ex)})
		# Abort instant demo if warm-up fails
		sys.exit(1)

# Derive attribute logical name prefix from the entity logical name (segment before first underscore)
attr_prefix = logical.split("_", 1)[0] if "_" in logical else logical
code_key = f"{attr_prefix}_code"
count_key = f"{attr_prefix}_count"
amount_key = f"{attr_prefix}_amount"
when_key = f"{attr_prefix}_when"
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
}
# Generate multiple payloads
multi_payloads: list[dict] = []
base_date = date(2025, 1, 2)
for i in range(1, 16):
	multi_payloads.append(
		{
			f"{attr_prefix}_name": f"Sample {i:02d}",
			code_key: f"X{200 + i:03d}",
			count_key: 5 * i,
			amount_key: round(10.0 * i, 2),
			when_key: (base_date + timedelta(days=i - 1)).isoformat(),
			f"{attr_prefix}_active": True,
		}
	)

record_ids: list[str] = []
created_recs: list[dict] = []

try:
	# Single create (always returns full representation)
	log_call(f"client.create('{entity_set}', single_payload)")
	# Retry in case the custom table isn't fully provisioned immediately (404)
	rec1 = backoff_retry(lambda: client.create(entity_set, single_payload))
	created_recs.append(rec1)
	rid1 = rec1.get(id_key)
	if rid1:
		record_ids.append(rid1)

	# Multi create (list) now returns list[str] of IDs
	log_call(f"client.create('{entity_set}', multi_payloads)")
	multi_ids = backoff_retry(lambda: client.create(entity_set, multi_payloads))
	if isinstance(multi_ids, list):
		for mid in multi_ids:
			if isinstance(mid, str):
				record_ids.append(mid)
	else:
		print({"multi_unexpected_type": type(multi_ids).__name__, "value_preview": str(multi_ids)[:300]})

	print({"entity": logical, "created_ids": record_ids})
	summaries = []
	for rec in created_recs:
		summaries.append({"id": rec.get(id_key), **summary_from_record(rec)})
	print_line_summaries("Created record summaries (single only; multi-create returns IDs only):", summaries)
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
		log_call(f"client.get('{entity_set}', '{target}')")
		rec = backoff_retry(lambda: client.get(entity_set, target))
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
	}
	expected_checks = {
		f"{attr_prefix}_code": "X002",
		f"{attr_prefix}_count": 99,
		f"{attr_prefix}_active": False,
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
	log_call(f"client.update('{entity_set}', '{target_id}', update_data)")
	new_rec = backoff_retry(lambda: client.update(entity_set, target_id, update_data))
	# Verify string/int/bool fields
	for k, v in expected_checks.items():
		assert new_rec.get(k) == v, f"Field {k} expected {v}, got {new_rec.get(k)}"
	# Verify decimal with tolerance
	got = new_rec.get(amount_key)
	got_f = float(got) if got is not None else None
	assert got_f is not None and abs(got_f - 543.21) < 1e-6, f"Field {amount_key} expected 543.21, got {got}"
	print({"entity": logical, "updated": True})
	print_line_summaries("Updated record summary:", [{"id": target_id, **summary_from_record(new_rec)}])
except Exception as e:
	print(f"Update/verify failed: {e}")
	sys.exit(1)
# 4) Query records via SQL Custom API
print("Query (SQL via Custom API):")
try:
	import time
	pause("Execute SQL Query")

	def _run_query():
		log_call(f"client.query_sql(\"SELECT TOP 2 * FROM {logical} ORDER BY {attr_prefix}_amount DESC\")")
		return client.query_sql(f"SELECT TOP 2 * FROM {logical} ORDER BY {attr_prefix}_amount DESC")

	def _retry_if(ex: Exception) -> bool:
		msg = str(ex) if ex else ""
		return ("Invalid table name" in msg) or ("Invalid object name" in msg)

	rows = backoff_retry(_run_query, delays=(0, 2, 5), retry_http_statuses=(), retry_if=_retry_if)
	id_key = f"{logical}id"
	ids = [r.get(id_key) for r in rows if isinstance(r, dict) and r.get(id_key)]
	print({"entity": logical, "rows": len(rows) if isinstance(rows, list) else 0, "ids": ids})
	tds_summaries = []
	for row in rows if isinstance(rows, list) else []:
		tds_summaries.append(
			{
				"id": row.get(id_key),
				"code": row.get(code_key),
				"count": row.get(count_key),
				"amount": row.get(amount_key),
				"when": row.get(when_key),
			}
		)
	print_line_summaries("TDS record summaries (top 2 by amount):", tds_summaries)
except Exception as e:
	print(f"SQL via Custom API failed: {e}")

# Pause between SQL query and retrieve-multiple demos
pause("Retrieve multiple (OData paging demos)")

# 4.5) Retrieve multiple via OData paging (scenarios)
def run_paging_demo(label: str, *, top: Optional[int], page_size: Optional[int]) -> None:
	print("")
	print({"paging_demo": label, "top": top, "page_size": page_size})
	total = 0
	page_index = 0
	_select = [id_key, code_key, amount_key, when_key]
	_orderby = [f"{code_key} asc"]
	for page in client.get_multiple(
		entity_set,
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
				{"id": r.get(id_key), "code": r.get(code_key), "amount": r.get(amount_key), "when": r.get(when_key)}
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
# Show deletes to be executed (concurrently via SDK delete)
if 'record_ids' in locals() and record_ids:
	print({"delete_count": len(record_ids)})
pause("Execute Delete (concurrent SDK calls)")
try:
	if record_ids:
		max_workers = min(8, len(record_ids))
		log_call(f"concurrent delete {len(record_ids)} items from '{entity_set}' (workers={max_workers})")

		successes: list[str] = []
		failures: list[dict] = []

		def _del_one(rid: str) -> tuple[str, bool, str | None]:
			try:
				log_call(f"client.delete('{entity_set}', '{rid}')")
				backoff_retry(lambda: client.delete(entity_set, rid))
				return (rid, True, None)
			except Exception as ex:
				return (rid, False, str(ex))

		with ThreadPoolExecutor(max_workers=max_workers) as executor:
			future_map = {executor.submit(_del_one, rid): rid for rid in record_ids}
			for fut in as_completed(future_map):
				rid, ok, err = fut.result()
				if ok:
					successes.append(rid)
				else:
					failures.append({"id": rid, "error": err})

		print({
			"entity": logical,
			"delete_summary": {"requested": len(record_ids), "success": len(successes), "failures": len(failures)},
			"failed": failures[:5],  # preview up to 5 failures
		})
	else:
		raise RuntimeError("No record created; skipping delete.")
except Exception as e:
	print(f"Delete failed: {e}")

# 6) (Optional) Instant create path demo
if not run_instant_create:
	print("Skipping instant create demo as per user choice.")
else:
	pause("Next: instant create demo")

	print("Instant create demo")
	print("Delete Instant table first if exists")
	# Delete instant table first
	delete_table_if_exists("new_SampleItemInstant")

	# Create Instant
	log_call("client.create_table('new_SampleItemInstant', instant_create)")
	instant_schema = {
		"code": "text",
		"count": "text",
	}
	# Demo dummy lookup definition (must supply at least one for instant path)
	instant_lookups = [
		{
			"AttributeName": "new_Account",
			"AttributeDisplayName": "Account (Demo Lookup)",
			"ReferencedEntityName": "account",
			"RelationshipName": "new_newSampleItem_account",
		}
	]
	try:
		_t0_instant = time.perf_counter()
		_table_instant = client.create_table(
			"new_SampleItemInstant",
			instant_schema,
			use_instant=True,
			display_name="Sample Item",
			lookups=instant_lookups,
		)
		instant_create_seconds = time.perf_counter() - _t0_instant
		table_info = _table_instant
		logical = table_info.get("entity_logical_name") if isinstance(table_info, dict) else None
		print(table_info)
	except Exception as instant_ex:
		print({"instant_create_error": str(instant_ex)})
		sys.exit(1)

	# Timing comparison summary for table creation
	_standard_create_ran = standard_create_seconds is not None
	_instant_create_ran = instant_create_seconds is not None
	print({
		"table_creation_timing_compare": {
			"warm_up_seconds": warm_up_seconds,
			"instant_seconds": instant_create_seconds,
			"warm_up+instant_seconds": warm_up_seconds + instant_create_seconds,
			"standard_seconds": standard_create_seconds if _standard_create_ran else "standard table pre-existed; omitted",
			"delta_standard_minus_instant": (
				(standard_create_seconds - instant_create_seconds)
				if (_standard_create_ran and _instant_create_ran)
				else None
			),
		}
	})


pause("Next: Cleanup table")

# 7) Cleanup: delete the custom table if it exists
print("Cleanup (Metadata):")
if delete_table_at_end:
	delete_table_if_exists("new_SampleItem")
else:
	print({"table_deleted": False, "reason": "user opted to keep table"})

# Put instant table delete at the end to avoid metadata cache issues when deletion immediately follows creation
if run_instant_create:
	print("Cleanup instant (Metadata):")
	delete_table_if_exists("new_SampleItemInstant")
