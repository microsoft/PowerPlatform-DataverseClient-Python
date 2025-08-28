import sys
from pathlib import Path
import traceback
import time
import requests

# Add src to PYTHONPATH for local runs; insert at position 0 so local code overrides any installed package
src_path = str(Path(__file__).resolve().parents[1] / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from dataverse_sdk import DataverseClient 
from azure.identity import InteractiveBrowserCredential

# ---------------- Configuration ----------------
base_url = "https://aurorabapenv0f528.crm10.dynamics.com"  # <-- change to your environment
CUSTOM_API_UNIQUE_NAME = "new_EchoMessage"     # Must be globally unique in the org
REQUEST_PARAM_UNIQUE = "new_EchoMessage_Message"
RESPONSE_PROP_UNIQUE = "new_EchoMessage_Response"
PUBLISH_STRATEGY = "auto"  # auto | skip | force. force = call PublishAllXml, auto = poll metadata first
# Parameter type codes (subset): 10=String, 7=Int32, 6=Float. Using Int32 for this run.
REQUEST_PARAMETERS = [{
    "uniquename": REQUEST_PARAM_UNIQUE,
    "name": "Message",
    "displayname": "Message",
    "type": 7,  # Int32
    "description": "Integer value to echo / raise event with",
    "isoptional": False,
}]
RESPONSE_PROPERTIES = [{
    "uniquename": RESPONSE_PROP_UNIQUE,
    "name": "ResponseMessage",
    "displayname": "ResponseMessage",
    "type": 10,  # String response
    "description": "Echoed string",
}]

# ------------------------------------------------
client = DataverseClient(base_url=base_url, credential=InteractiveBrowserCredential())
odata = client._get_odata()  # low-level client exposing custom API helpers

# Small helpers: call logging and step pauses

def log_call(call: str) -> None:
    print({"call": call})

def plan(call: str) -> None:
    print({"plan": call})

# Simple generic backoff (same style as other quickstarts)

def backoff_retry(op, *, delays=(0, 2, 5), retry_http_statuses=(429, 500, 502, 503, 504)):
    last_exc = None
    for d in delays:
        if d:
            time.sleep(d)
        try:
            return op()
        except Exception as ex:
            last_exc = ex
            if isinstance(ex, requests.exceptions.HTTPError):
                code = getattr(getattr(ex, "response", None), "status_code", None)
                if code in retry_http_statuses:
                    continue
            break
    if last_exc:
        raise last_exc

# 1) Check if target Custom API exists
print("Check target Custom API existence:")
try:
    plan("odata.get_custom_api(unique_name)")
    existing = backoff_retry(lambda: odata.get_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME))
    print({"exists": bool(existing)})
except Exception as e:
    print(f"Existence check failed: {e}")

# 2) Create the Custom API, remove the existing one first if present
print("Recreate Custom API fresh (delete if exists then create):")
existing_api = odata.get_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME)
if existing_api:
    plan("odata.delete_custom_api(existing)")
    try:
        backoff_retry(lambda: odata.delete_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME))
        print({"deleted_prior": True})
        # Brief pause to allow backend cleanup
        time.sleep(2)
    except Exception as del_ex:
        print({"delete_prior_error": str(del_ex)})

plan("odata.create_custom_api (inline request parameter + response property)")
try:
    api_meta = backoff_retry(lambda: odata.create_custom_api(
        unique_name=CUSTOM_API_UNIQUE_NAME,
        name="Echo Message",
        description="Echo sample (metadata only) created by SDK quickstart.",
        is_function=False,
        binding_type="Global",
        request_parameters=REQUEST_PARAMETERS,
        response_properties=RESPONSE_PROPERTIES,
    ))
    print({
        "created": True,
        "message": "Created Custom API with the following parameters",
        "unique_name": CUSTOM_API_UNIQUE_NAME,
        "customapiid": api_meta.get("customapiid"),
        "description": "Echo sample (metadata only) created by SDK quickstart.",
        "is_function": False,
        "request_parameters": [p.get("name") for p in REQUEST_PARAMETERS],
        "response_properties": [p.get("name") for p in RESPONSE_PROPERTIES]
    })
except Exception as e:
    print("Create Custom API failed:")
    traceback.print_exc()
    resp = getattr(e, 'response', None)
    if resp is not None:
        try:
            print({"status": resp.status_code, "body": resp.text[:2000]})
        except Exception:
            pass
    sys.exit(1)

customapiid = api_meta.get("customapiid") if api_meta else None
if not customapiid:
    print("Missing customapiid; cannot continue")
    sys.exit(1)

# 3) Read back the Custom API metadata just created
print("Read Custom API metadata:")
try:
    plan("odata.get_custom_api(unique_name)")
    read_back = backoff_retry(lambda: odata.get_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME))
    if read_back:
        # Display a concise subset of fields
        subset = {k: read_back.get(k) for k in [
            "customapiid", "uniquename", "isfunction", "bindingtype", "allowedcustomprocessingsteptype", "isprivate", "executeprivilegename", "description"
        ]}
        subset["request_param_count"] = len(REQUEST_PARAMETERS)
        subset["response_prop_count"] = len(RESPONSE_PROPERTIES)
        print({"read_back": subset})
    else:
        print({"read_back": None})
except Exception as e:
    print({"read_custom_api_error": str(e)})

# Publish customizations so the action metadata is available for invocation (required for freshly created APIs)
print("Ensure custom API metadata is available:")

def _action_in_metadata(action_name: str) -> bool:
    try:
        md_resp = odata._request(
            "get",
            f"{odata.api}/$metadata",
            headers={**odata._headers(), "Accept": "application/xml"},
        )
        if md_resp.status_code == 200:
            txt = md_resp.text
            return f"Name=\"{action_name}\"" in txt
    except Exception:
        return False
    return False

def wait_for_action(action_name: str, timeout_sec: int = 60, interval: float = 2.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_sec:
        if _action_in_metadata(action_name):
            return True
        time.sleep(interval)
    return _action_in_metadata(action_name)

published = False
if PUBLISH_STRATEGY == "skip":
    print({"publish_strategy": "skip"})
elif PUBLISH_STRATEGY in ("auto", "force"):
    if PUBLISH_STRATEGY == "auto":
        # First attempt: see if already present (often immediate)
        if _action_in_metadata(CUSTOM_API_UNIQUE_NAME):
            print({"publish_strategy": "auto", "metadata_present": True})
            published = True
        else:
            print({"publish_strategy": "auto", "metadata_present": False, "action": "polling"})
            if wait_for_action(CUSTOM_API_UNIQUE_NAME, timeout_sec=20, interval=2):
                print({"metadata_present_after_poll": True})
                published = True
    # Fallback (auto when still not present, or explicit force): attempt PublishAllXml with timeout
    if not published:
        try:
            plan("POST PublishAllXml (timeout=15s)")
            pub_url = f"{odata.api}/PublishAllXml"
            # Direct requests call so we can enforce timeout
            r_pub = requests.post(pub_url, headers=odata._headers(), json={}, timeout=15)
            if r_pub.status_code not in (200, 204):
                r_pub.raise_for_status()
            print({"published": True, "status": r_pub.status_code})
            # Short propagation wait + poll again
            time.sleep(3)
            if wait_for_action(CUSTOM_API_UNIQUE_NAME, timeout_sec=25, interval=2):
                print({"metadata_present_after_publish": True})
                published = True
            else:
                print({"metadata_present_after_publish": False, "hint": "Invocation retry logic will attempt anyway."})
        except requests.exceptions.Timeout:
            print({"published": False, "error": "PublishAllXml timeout (15s)", "hint": "Proceeding; action may still become available."})
        except Exception as pub_ex:
            print({"published": False, "error": str(pub_ex)})
else:
    print({"publish_strategy": PUBLISH_STRATEGY, "warning": "Unknown strategy value"})

# 4) (Re)List parameters / response properties for visibility
print("List Parameters / Response Properties:")
try:
    params = odata.list_custom_api_request_parameters(customapiid)
    props = odata.list_custom_api_response_properties(customapiid)
    print({"parameters": [p.get("name") for p in params], "responses": [p.get("name") for p in props]})
except Exception as e:
    print(f"List params/props failed: {e}")

# 5) Invoke the Custom API
print("Invoke Custom API:")
try:
    base_message = 42  # Matches Int32 parameter type
    candidate_param_names = [REQUEST_PARAM_UNIQUE]
    last_error = None
    for pname in candidate_param_names:
        for attempt in range(1,4):  # up to 3 attempts each name for propagation / publish delay
            invoke_payload = {pname: base_message}
            plan(f"attempt {attempt} param '{pname}' -> odata.call_custom_api('{CUSTOM_API_UNIQUE_NAME}', {invoke_payload})")
            def invoke():
                return odata.call_custom_api(CUSTOM_API_UNIQUE_NAME, invoke_payload)
            try:
                result = invoke()
                print({"invoked": True, "message": "note the None in new_EchoMessage_Response is expected as there is no server logic attached to the workflow", "result": result, "used_param": pname, "attempt": attempt})
                raise SystemExit  # exit double loop cleanly
            except requests.exceptions.HTTPError as ex:
                last_error = ex
                resp = getattr(ex, 'response', None)
                status = getattr(resp, 'status_code', None)
                body = None
                if resp is not None:
                    try:
                        body = resp.text[:600]
                    except Exception:
                        body = None
                body_lc = (body or "").lower()
                # Handle not yet routable (sdkmessage) 404 specially
                if status == 404 and 'sdkmessage' in body_lc:
                    print({"retry": True, "reason": "404 sdkmessage not found (known issue where the custom api exists but metadata is not updated yet)", "attempt": attempt})
                    time.sleep(2 + attempt)
                    continue
                if status == 400 and "not a valid parameter" in body_lc:
                    time.sleep(2 + attempt)
                    continue
                if status == 400 and "int32" in body_lc:
                    print({"hint": "Server expects Int32; payload is int. Likely metadata publish delay."})
                    time.sleep(2)
                    continue
                print({"attempt": pname, "error": str(ex), "status": status, "body": body})
                time.sleep(2)
                continue
    if last_error:
        raise last_error
except SystemExit:
    pass  # Successful invocation path signaled via SystemExit raise above
except Exception as e:  # Invocation may legitimately fail without a plug-in
    resp = getattr(e, 'response', None)
    body = None
    if resp is not None:
        try:
            body = resp.text[:1500]
        except Exception:
            body = None
    print({"invoked": False, "error": str(e), "body": body, "hint": "If 400, verify parameter Type code & payload match; for plug-in-less mode only request param should be present."})

# 6) Update custom api
print("Update Custom API:")
try:
    plan("odata.update_custom_api(unique_name, changes={'description': 'Updated via quickstart'})")
    updated = backoff_retry(lambda: odata.update_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME, changes={"description": "Updated via quickstart"}))
    print({"updated": True, "description": updated.get("description")})
except Exception as e:
    print({"updated": False, "error": str(e)})

# 7) Cleanup
print("Cleanup: delete Custom API created in this run")
try:
    plan("odata.delete_custom_api(unique_name)")
    backoff_retry(lambda: odata.delete_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME))
    print({"deleted": True})
except Exception as e:
    print({"deleted": False, "error": str(e)})
