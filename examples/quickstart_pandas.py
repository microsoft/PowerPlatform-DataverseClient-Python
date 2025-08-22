import sys
from pathlib import Path

# Add src to PYTHONPATH for local runs
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from dataverse_sdk import DataverseClient
from dataverse_sdk.odata_pandas_wrappers import PandasODataClient
from azure.identity import InteractiveBrowserCredential
import traceback
import requests
import time
import pandas as pd

base_url = 'https://aurorabapenv0f528.crm10.dynamics.com'
client = DataverseClient(base_url=base_url, credential=InteractiveBrowserCredential())
# Use the internal OData client for pandas helpers
PANDAS = PandasODataClient(client._get_odata())

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

print("(Pandas) Ensure custom table exists (Metadata):")
table_info = None
created_this_run = False

# First check for existing table
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
# Derive attribute logical name prefix from the entity logical name
attr_prefix = logical.split("_", 1)[0] if "_" in logical else logical
record_data = {
	f"{attr_prefix}_name": "Sample X",
	f"{attr_prefix}_code": "X001",
	f"{attr_prefix}_count": 42,
	f"{attr_prefix}_amount": 123.45,
	f"{attr_prefix}_when": "2025-01-01",
	f"{attr_prefix}_active": True,
}

# 2) Create a record in the new table
print("(Pandas) Create record (OData via Pandas wrapper):")
record_id = None
try:
	record_id = backoff_retry(lambda: PANDAS.create_df(entity_set, pd.Series(record_data)))
	print({"entity": logical, "created_id": record_id})
except Exception as e:
	print(f"Create failed: {e}")
	sys.exit(1)

# 3) Read record via OData
print("(Pandas) Read (OData via Pandas wrapper):")
try:
	if record_id:
		# get_ids returns a DataFrame; fetch single row
		df = backoff_retry(lambda: PANDAS.get_ids(entity_set, pd.Series([record_id])))
		print(df.head())
		id_key = f"{logical}id"
		rid = df.iloc[0].get(id_key) if not df.empty else None
		print({"entity": logical, "read": True, "id": rid})
	else:
		raise RuntimeError("No record created; skipping read.")
except Exception as e:
	print(f"Get failed: {e}")

# 3.5) Update record, then read again and verify
print("(Pandas) Update (OData via Pandas wrapper) and verify:")
try:
	if not record_id:
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

	# Perform update via Pandas wrapper (returns None), then re-fetch to verify
	backoff_retry(lambda: PANDAS.update(entity_set, record_id, pd.Series(update_data)))
	print({"entity": logical, "updated": True})

	# Re-read and verify from DataFrame
	after_df = backoff_retry(lambda: PANDAS.get_ids(entity_set, pd.Series([record_id])))
	row = after_df.iloc[0] if not after_df.empty else {}

	# Verify string/int/bool fields
	for k, v in expected_checks.items():
		gv = row.get(k) if hasattr(row, 'get') else None
		assert gv == v, f"Field {k} expected {v}, got {gv}"

	# Verify decimal with tolerance
	got = row.get(amount_key) if hasattr(row, 'get') else None
	got_f = float(got) if got is not None else None
	assert got_f is not None and abs(got_f - 543.21) < 1e-6, f"Field {amount_key} expected 543.21, got {got}"

	print({"entity": logical, "verified": True})
except Exception as e:
	print(f"Update/verify failed: {e}")
	sys.exit(1)

# 4) Query records via SQL Custom API
print("(Pandas) Query (SQL via Custom API):")
try:
	# Try singular logical name first, then plural entity set, with short backoff
	import time

	candidates = [logical]
	if entity_set and entity_set != logical:
		candidates.append(entity_set)

	df_rows = None
	for name in candidates:
		def _run_query():
			return PANDAS.query_sql_df(f"SELECT TOP 3 * FROM {name} ORDER BY createdon DESC")
		def _retry_if(ex: Exception) -> bool:
			msg = str(ex) if ex else ""
			return ("Invalid table name" in msg) or ("Invalid object name" in msg)
		try:
			df_rows = backoff_retry(_run_query, delays=(0, 2, 5), retry_http_statuses=(), retry_if=_retry_if)
			id_key = f"{logical}id"
			ids = df_rows[id_key].dropna().tolist() if (df_rows is not None and id_key in df_rows.columns) else []
			print({"entity": name, "rows": (0 if df_rows is None else len(df_rows)), "ids": ids})
			raise SystemExit
		except Exception:
			continue
except SystemExit:
	pass
except Exception as e:
	print(f"SQL via Custom API failed: {e}")

# 5) Delete record
print("(Pandas) Delete (OData via Pandas wrapper):")
try:
	if record_id:
		backoff_retry(lambda: PANDAS.delete_ids(entity_set, record_id))
		print({"entity": logical, "deleted": True})
	else:
		raise RuntimeError("No record created; skipping delete.")
except Exception as e:
	print(f"Delete failed: {e}")

# 6) Cleanup: delete the custom table if it exists
print("Cleanup (Metadata):")
try:
	# Delete if present, regardless of whether it was created in this run
	info = client.get_table_info("SampleItem")
	if info:
		client.delete_table("SampleItem")
		print({"table_deleted": True})
	else:
		print({"table_deleted": False, "reason": "not found"})
except Exception as e:
	print(f"Delete table failed: {e}")
