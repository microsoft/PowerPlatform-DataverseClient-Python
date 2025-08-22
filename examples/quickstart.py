import sys
from pathlib import Path

# Add src to PYTHONPATH for local runs
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from dataverse_sdk import DataverseClient
from azure.identity import InteractiveBrowserCredential
import traceback
import requests
import time
 
base_url = 'https://aurorabapenv0f528.crm10.dynamics.com'
client = DataverseClient(base_url=base_url, credential=InteractiveBrowserCredential())

# Small helpers: call logging and step pauses
def log_call(call: str) -> None:
	print({"call": call})

def pause(next_step: str) -> None:
	# No-op (env-free quickstart)
	return

def plan_call(call: str) -> None:
	print({"plan": call})

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

# First check for existing table
log_call("client.get_table_info('SampleItem')")
existing = client.get_table_info("SampleItem")
if existing:
	table_info = existing
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
		log_call("client.create_table('SampleItem', schema={code,count,amount,when,active})")
		table_info = client.create_table(
			"SampleItem",
			{
				"code": "string",
				"count": "int",
				"amount": "decimal",
				"when": "datetime",
				"active": "bool",
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
entity_set = table_info.get("entity_set_name")
logical = table_info.get("entity_logical_name") or entity_set.rstrip("s")

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
print("Create records (OData):")
# Show planned creates before executing
for _ in range(3):
	plan_call(f"client.create('{entity_set}', payload)")
pause("Execute Create")
record_ids: list[str] = []
created_recs: list[dict] = []
create_payloads = [
	{
		f"{attr_prefix}_name": "Sample A",
		code_key: "X001",
		count_key: 42,
		amount_key: 123.45,
		when_key: "2025-01-01",
		f"{attr_prefix}_active": True,
	},
	{
		f"{attr_prefix}_name": "Sample B",
		code_key: "X002",
		count_key: 7,
		amount_key: 987.65,
		when_key: "2025-01-02",
		f"{attr_prefix}_active": True,
	},
	{
		f"{attr_prefix}_name": "Sample C",
		code_key: "X003",
		count_key: 100,
		amount_key: 222.22,
		when_key: "2025-01-03",
		f"{attr_prefix}_active": False,
	},
]

try:
	for payload in create_payloads:
		log_call(f"client.create('{entity_set}', payload)")
		rec = backoff_retry(lambda p=payload: client.create(entity_set, p))
		created_recs.append(rec)
		rid = rec.get(id_key)
		if rid:
			record_ids.append(rid)
	print({"entity": logical, "created_ids": record_ids})
	# Summarize the created records from the returned payloads
	summaries = []
	for rec in created_recs:
		summaries.append({"id": rec.get(id_key), **summary_from_record(rec)})
	print_line_summaries("Created record summaries:", summaries)
except Exception as e:
	print(f"Create failed: {e}")
	sys.exit(1)

pause("Next: Read record")

# 3) Read record via OData
print("Read (OData):")
# Show planned reads before executing
if 'record_ids' in locals() and record_ids:
	for rid in record_ids:
		plan_call(f"client.get('{entity_set}', '{rid}')")
pause("Execute Read")
try:
	if record_ids:
		summaries = []
		for rid in record_ids:
			log_call(f"client.get('{entity_set}', '{rid}')")
			rec = backoff_retry(lambda r=rid: client.get(entity_set, r))
			summaries.append({"id": rid, **summary_from_record(rec)})
		print_line_summaries("Read record summaries:", summaries)
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
	plan_call(f"client.update('{entity_set}', '{target_id}', update_data)")
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
	# Try singular logical name first, then plural entity set, with short backoff
	import time

	candidates = [logical]
	if entity_set and entity_set != logical:
		candidates.append(entity_set)

	# Show planned SQL queries before executing
	for name in candidates:
		plan_call(f"client.query_sql(\"SELECT TOP 2 * FROM {name} ORDER BY {attr_prefix}_amount DESC\")")
	pause("Execute SQL Query")

	rows = []
	for name in candidates:
		def _run_query():
			log_call(f"client.query_sql(\"SELECT TOP 2 * FROM {name} ORDER BY {attr_prefix}_amount DESC\")")
			return client.query_sql(f"SELECT TOP 2 * FROM {name} ORDER BY {attr_prefix}_amount DESC")
		def _retry_if(ex: Exception) -> bool:
			msg = str(ex) if ex else ""
			return ("Invalid table name" in msg) or ("Invalid object name" in msg)
		try:
			rows = backoff_retry(_run_query, delays=(0, 2, 5), retry_http_statuses=(), retry_if=_retry_if)
			logical_for_ids = logical
			id_key = f"{logical_for_ids}id"
			ids = [r.get(id_key) for r in rows if isinstance(r, dict) and r.get(id_key)]
			print({"entity": name, "rows": len(rows) if isinstance(rows, list) else 0, "ids": ids})
			# Print TDS summaries for clarity
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
			raise SystemExit
		except Exception:
			continue
except SystemExit:
	pass
except Exception as e:
	print(f"SQL via Custom API failed: {e}")
# 5) Delete record
print("Delete (OData):")
# Show planned deletes before executing
if 'record_ids' in locals() and record_ids:
	for rid in record_ids:
		plan_call(f"client.delete('{entity_set}', '{rid}')")
pause("Execute Delete")
try:
	if record_ids:
		for rid in record_ids:
			log_call(f"client.delete('{entity_set}', '{rid}')")
			backoff_retry(lambda r=rid: client.delete(entity_set, r))
		print({"entity": logical, "deleted_ids": record_ids})
	else:
		raise RuntimeError("No record created; skipping delete.")
except Exception as e:
	print(f"Delete failed: {e}")

pause("Next: Cleanup table")

# 6) Cleanup: delete the custom table if it exists
print("Cleanup (Metadata):")
try:
	# Delete if present, regardless of whether it was created in this run
	log_call("client.get_table_info('SampleItem')")
	info = client.get_table_info("SampleItem")
	if info:
		log_call("client.delete_table('SampleItem')")
		client.delete_table("SampleItem")
		print({"table_deleted": True})
	else:
		print({"table_deleted": False, "reason": "not found"})
except Exception as e:
	print(f"Delete table failed: {e}")
