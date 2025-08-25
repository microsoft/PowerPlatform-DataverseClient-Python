import sys
from pathlib import Path
import traceback
import time
import requests

# Add src to PYTHONPATH for local runs; insert at position 0 so local code overrides any installed package
src_path = str(Path(__file__).resolve().parents[1] / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from dataverse_sdk import DataverseClient  # noqa: E402
from azure.identity import InteractiveBrowserCredential  # noqa: E402

"""Quickstart: Custom API lifecycle (create -> add params -> invoke -> update -> delete).

Two operating modes:
1. Plug-in backed (you supply PLUGIN_TYPENAME): request + response property, plug-in sets the output.
2. Plug-in-less (business event style): ONLY a request parameter is created. Invocation will succeed
    (HTTP 204 / empty body or {{}}) even though no plug-in logic runs. This matches docs stating a
    custom API does not strictly require a plug-in (it can just raise events). In this mode we omit
    response properties because without server logic they cannot be populated and may cause confusion.

Below we auto-detect: if PLUGIN_TYPENAME is blank we skip creating the response property and use a
simple string parameter value. If a plug-in name is provided we also create a response property.
"""

# ---------------- Configuration ----------------
base_url = "https://aurorabapenv0f528.crm10.dynamics.com"  # <-- change to your environment
CUSTOM_API_UNIQUE_NAME = "new_EchoMessage"     # Must be globally unique in the org
REQUEST_PARAM_UNIQUE = "new_EchoMessage_Message"
RESPONSE_PROP_UNIQUE = "new_EchoMessage_Response"
CLEANUP = True  # Set True to delete the Custom API at the end
PLUGIN_TYPENAME = ""  # e.g. "Contoso.Plugins.EchoMessagePlugin" (leave blank for plug-in-less mode)
INCLUDE_RESPONSE_PROPERTY = bool(PLUGIN_TYPENAME)  # Only create a response property when a plug-in can populate it
PUBLISH_STRATEGY = "auto"  # auto | skip | force. force = call PublishAllXml, auto = poll metadata first

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
        except Exception as ex:  # noqa: BLE001
            last_exc = ex
            if isinstance(ex, requests.exceptions.HTTPError):
                code = getattr(getattr(ex, "response", None), "status_code", None)
                if code in retry_http_statuses:
                    continue
            break
    if last_exc:
        raise last_exc

# 1) List existing custom APIs with our prefix for context
print("List existing custom APIs (prefix=new_):")
try:
    plan("odata.list_custom_apis(filter_expr=uniquename startswith 'new_')")
    existing = backoff_retry(lambda: odata.list_custom_apis(filter_expr="startswith(uniquename,'new_')"))
    print({"count": len(existing)})
    for item in existing[:5]:  # show a few
        print(" -", item.get("uniquename"), "isfunction=" + str(item.get("isfunction")))
except Exception as e:  # noqa: BLE001
    print(f"List custom APIs failed: {e}")

# 2) Create the Custom API if absent
print("Recreate Custom API fresh (delete if exists then create):")
existing_api = odata.get_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME)
if existing_api:
    plan("odata.delete_custom_api(existing)")
    try:
        backoff_retry(lambda: odata.delete_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME))
        print({"deleted_prior": True})
        # Brief pause to allow backend cleanup
        time.sleep(2)
    except Exception as del_ex:  # noqa: BLE001
        print({"delete_prior_error": str(del_ex)})

plan("odata.create_custom_api (inline request parameter + optional response property)")
try:
    # Parameter type codes (subset): 10=String, 7=Integer, 6=Float. We use String for plug-in-less clarity.
    request_parameters = [{
        "uniquename": REQUEST_PARAM_UNIQUE,
        "name": "Message",
        "displayname": "Message",
        "type": 6,  # Int32 (common & simple)
        "description": "Integer message to echo / raise event with",
        "isoptional": False,
    }]
    response_properties = []
    if INCLUDE_RESPONSE_PROPERTY:
        response_properties.append({
            "uniquename": RESPONSE_PROP_UNIQUE,
            "name": "ResponseMessage",
            "displayname": "ResponseMessage",
            "type": 6,  # Int32 response
            "description": "Echoed integer (set by plug-in)",
        })

    api_meta = backoff_retry(lambda: odata.create_custom_api(
        unique_name=CUSTOM_API_UNIQUE_NAME,
        name="Echo Message",
        description="Echo sample (metadata only) created by SDK quickstart.",
        is_function=False,
        binding_type="Global",
        request_parameters=request_parameters,
        response_properties=response_properties,
    ))
    print({"created": True, "customapiid": api_meta.get("customapiid")})
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
created_this_run = True

customapiid = api_meta.get("customapiid") if api_meta else None
if not customapiid:
    print("Missing customapiid; cannot continue")
    sys.exit(1)

# Publish customizations so the action metadata is available for invocation (required for freshly created APIs)
print("Ensure custom API metadata is available:")

def _action_in_metadata(action_name: str) -> bool:
    try:
        # Must include auth headers; previously we overwrote them causing 401
        md_resp = odata._request(
            "get",
            f"{odata.api}/$metadata",
            headers={**odata._headers(), "Accept": "application/xml"},
        )
        if md_resp.status_code == 200:
            txt = md_resp.text
            return f"Name=\"{action_name}\"" in txt
    except Exception:  # noqa: BLE001
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
        except Exception as pub_ex:  # noqa: BLE001
            print({"published": False, "error": str(pub_ex)})
else:
    print({"publish_strategy": PUBLISH_STRATEGY, "warning": "Unknown strategy value"})

# Optional: bind an existing plug-in type so invocation returns something meaningful.
# Provide the fully-qualified class name in PLUGIN_TYPENAME above. The plug-in must
# set OutputParameters["ResponseMessage"] (or whatever your response property name is).
if PLUGIN_TYPENAME:
    print("Attempt plug-in bind:")
    try:
        plan(f"lookup plugintype '{PLUGIN_TYPENAME}' then patch custom api")
        # Lookup plugintypeid by typename
        url = f"{odata.api}/plugintypes"
        params = {"$select": "plugintypeid,typename", "$filter": f"typename eq '{PLUGIN_TYPENAME}'"}
        r = odata._request("get", url, headers=odata._headers(), params=params)
        r.raise_for_status()
        vals = r.json().get("value", [])
        if vals:
            plugintypeid = vals[0]["plugintypeid"]
            log_call("odata.update_custom_api (attach plugintype)")
            patched = odata.update_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME, changes={
                "plugintypeid@odata.bind": f"/plugintypes({plugintypeid})"
            })
            print({"plugin_attached": True, "plugintypeid": plugintypeid})
        else:
            print({"plugin_attached": False, "reason": "Plugin typename not found"})
    except Exception as ex:  # noqa: BLE001
        resp = getattr(ex, 'response', None)
        body = None
        if resp is not None:
            try:
                body = resp.text[:800]
            except Exception:  # noqa: BLE001
                body = None
        print({"plugin_attach_error": str(ex), "body": body})

# 3) (Re)List parameters / response properties for visibility
print("Parameters / Response Properties:")
try:
    params = odata.list_custom_api_request_parameters(customapiid)
    props = odata.list_custom_api_response_properties(customapiid)
    print({"parameters": [p.get("name") for p in params], "responses": [p.get("name") for p in props]})
except Exception as e:  # noqa: BLE001
    print(f"List params/props failed: {e}")

# 4) Invoke the Custom API (will only succeed if backed by server logic)
print("Invoke Custom API:")
time.sleep(1)
try:
    # Fetch $metadata to inspect the expected parameter names (diagnostic aid)
    try:
        meta_url = f"{odata.api}/$metadata"
        md_resp = odata._request(
            "get",
            meta_url,
            headers={**odata._headers(), "Accept": "application/xml"},
        )
        md_resp.raise_for_status()
        md_text = md_resp.text
        # Extract the Action definition snippet for debugging
        snippet = None
        idx = md_text.find(f"Name=\"{CUSTOM_API_UNIQUE_NAME}\"")
        if idx != -1:
            snippet = md_text[max(0, idx-200): idx+400]
        if snippet:
            print({"metadata_snippet": snippet.replace('\n', ' ')[:400]})
    except Exception as md_ex:  # noqa: BLE001
        print({"metadata_fetch_error": str(md_ex)})

    base_message = 123  # Int matches parameter type 6
    # Prefer the unique name first (proved to work in plug-in-less mode); keep logical name fallback for completeness
    candidate_param_names = [REQUEST_PARAM_UNIQUE, "Message"]
    last_error = None
    for pname in candidate_param_names:
        for attempt in range(1,4):  # up to 3 attempts each name for propagation
            invoke_payload = {pname: base_message}
            plan(f"attempt {attempt} param '{pname}' -> odata.call_custom_api('{CUSTOM_API_UNIQUE_NAME}', {invoke_payload})")
            def invoke():
                return odata.call_custom_api(CUSTOM_API_UNIQUE_NAME, invoke_payload)
            try:
                result = invoke()
                print({"invoked": True, "result": result, "mode": "plugin" if INCLUDE_RESPONSE_PROPERTY else "plugin-less", "used_param": pname, "attempt": attempt})
                raise SystemExit  # exit double loop cleanly
            except requests.exceptions.HTTPError as ex:  # noqa: PERF203
                last_error = ex
                resp = getattr(ex, 'response', None)
                body = None
                if resp is not None:
                    try:
                        body = resp.text[:300]
                    except Exception:  # noqa: BLE001
                        body = None
                if resp is not None and resp.status_code == 400 and "not a valid parameter" in (body or ""):
                    # Wait and retry
                    time.sleep(2 + attempt)
                    continue
                if resp is not None and resp.status_code == 400 and "Int32" in (body or ""):
                    print({"hint": "Server expects Int32; ensure payload is int (it is). If still failing, metadata not published yet."})
                    time.sleep(2)
                    continue
                print({"attempt": pname, "error": str(ex), "body": body})
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

# 5) Update description (demonstrate patch)
print("Update Custom API description:")
try:
    plan("odata.update_custom_api(unique_name, changes={'description': 'Updated via quickstart'})")
    updated = backoff_retry(lambda: odata.update_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME, changes={"description": "Updated via quickstart"}))
    print({"updated": True, "description": updated.get("description")})
except Exception as e:  # noqa: BLE001
    print({"updated": False, "error": str(e)})

# 6) Conditional cleanup
if CLEANUP and created_this_run:
    print("Cleanup: delete Custom API created in this run")
    try:
        plan("odata.delete_custom_api(unique_name)")
        backoff_retry(lambda: odata.delete_custom_api(unique_name=CUSTOM_API_UNIQUE_NAME))
        print({"deleted": True})
    except Exception as e:  # noqa: BLE001
        print({"deleted": False, "error": str(e)})
else:
    print({"cleanup": False, "reason": "CLEANUP flag False or pre-existing API"})
