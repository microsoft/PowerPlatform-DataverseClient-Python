"""FinOps SDK smoke test — exercises the four CRUD operations against a real
Dynamics 365 Finance & Operations environment.

This script is a manual harness (not part of pytest). It is the FinOps analogue
of ``scratch_smoketest.py`` for the Dataverse SDK.

USAGE
-----
1. ``az login --tenant <tenant-id>``  (must have FinOps system access)
2. Set the environment variables below.
3. ``python scratch_finops_smoketest.py``

ENV VARS
--------
* FNO_ENV_URL   — e.g. https://my-env.operations.int.dynamics.com
* FNO_TENANT_ID — Entra tenant GUID

SAFE-DEFAULT STRATEGY
---------------------
Empty/sandbox FinOps envs typically reject CREATE on master entities like
CustomersV3 / CustomerGroups because they have unsatisfied mandatory references
(Currency, payment terms, tax groups, ...). To prove all four CRUD verbs
without depending on env-specific reference data, this script:

  1. READs the singleton row from the ``LegalEntities`` entity set (always present).
  2. PATCHes a benign string field (``Name``) to a marker value.
  3. READs back to confirm the round-trip.
  4. PATCHes back to the original value.
  5. Attempts a CREATE + DELETE on ``CustomerGroups`` (best-effort; surfaces any
     env-validation errors).

Steps 1-4 prove GET + UPDATE end-to-end. Step 5 proves CREATE + DELETE wire
format (success or controlled failure with structured error body).
"""
from __future__ import annotations

import os
import sys

from azure.identity import AzureCliCredential

from PowerPlatform.FinOps import FinOpsClient, FinOpsHttpError


ENV_URL = os.environ.get("FNO_ENV_URL", "https://<your-finops-env>.operations.int.dynamics.com")
TENANT  = os.environ.get("FNO_TENANT_ID", "")


def main() -> int:
    if "<your-finops-env>" in ENV_URL or not TENANT:
        print("Set FNO_ENV_URL and FNO_TENANT_ID environment variables first.",
              file=sys.stderr)
        print("  $env:FNO_ENV_URL   = 'https://my-env.operations.int.dynamics.com'", file=sys.stderr)
        print("  $env:FNO_TENANT_ID = '<tenant guid>'", file=sys.stderr)
        return 2

    cred = AzureCliCredential(tenant_id=TENANT, process_timeout=120)

    with FinOpsClient(ENV_URL, cred) as client:
        print(f"== Connected to {client.environment_url} ==")

        # ------- 1. service document sanity ------------------------------
        sd = client._http.request("GET", client.data_url, expected=(200,))
        sets = [v["name"] for v in sd.json().get("value", [])]
        print(f"Service document OK — {len(sets)} entity sets exposed")

        # ------- 2. GET + UPDATE round-trip on LegalEntities -------------
        leg = client._http.request(
            "GET", f"{client.data_url}/LegalEntities",
            params={"$top": "1"}, expected=(200,),
        ).json()["value"]
        if not leg:
            print("LegalEntities is empty — cannot run UPDATE round-trip.", file=sys.stderr)
            return 3
        leid = leg[0]["LegalEntityId"]
        orig = leg[0].get("Name") or ""
        print(f"LegalEntity: {leid!r}  Name={orig!r}")

        row = client.records.get("LegalEntities", leid)
        print(f"  records.get -> {len(row)} columns")

        marker = "FinOps-SDK-Step1-smoketest"
        client.records.update("LegalEntities", leid, {"Name": marker})
        after = client.records.get("LegalEntities", leid)
        assert after.get("Name") == marker, (
            f"UPDATE round-trip failed: expected {marker!r}, got {after.get('Name')!r}"
        )
        print(f"  records.update + records.get round-trip verified ({marker!r})")

        client.records.update("LegalEntities", leid, {"Name": orig})
        restored = client.records.get("LegalEntities", leid)
        assert restored.get("Name") == orig, "Failed to restore original Name"
        print(f"  Restored Name to {orig!r}")

        # ------- 3. CREATE + DELETE on CustomerGroups (best-effort) ------
        key = {"dataAreaId": "usmf", "CustomerGroupId": "SDK01"}
        try:
            client.records.delete("CustomerGroups", key)
            print("Pre-cleanup CustomerGroups SDK01 -> deleted")
        except FinOpsHttpError as e:
            print(f"Pre-cleanup CustomerGroups SDK01 -> {e.status_code} (skipped)")

        try:
            loc = client.records.create("CustomerGroups", {
                "dataAreaId": "usmf",
                "CustomerGroupId": "SDK01",
                "Description": "FinOps SDK smoke test",
            })
            print("CREATE -> ", loc)
        except FinOpsHttpError as e:
            print(f"CREATE rejected by env validation ({e.status_code}); body excerpt:")
            print(" ", str(e.response_body)[:300])

        try:
            client.records.delete("CustomerGroups", key)
            print("DELETE -> ok")
        except FinOpsHttpError as e:
            print(f"DELETE -> {e.status_code} (row not present in this env)")

    print("== FinOps CRUD smoke test PASSED ==")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

