"""Step 2 live smoketest — list pagination + metadata reads.

Run from the C:\\finops-crud-demo .venv with the SDK installed editable.
Requires: az login (see README), then `python step2_smoketest.py`.
"""
from __future__ import annotations

import os
import sys

from azure.identity import AzureCliCredential

from PowerPlatform.FinOps import FinOpsClient


def main() -> int:
    env_url = os.environ.get(
        "FNO_ENV_URL", "https://aurorabapenvdc9e7.operations.int.dynamics.com"
    )
    tenant_id = os.environ.get(
        "FNO_TENANT_ID", "4abc24ea-2d0b-4011-87d4-3de32ca1e9cc"
    )

    print("=" * 72)
    print(f" Step 2 smoketest against {env_url}")
    print("=" * 72)

    cred = AzureCliCredential(tenant_id=tenant_id)
    with FinOpsClient(env_url, cred) as client:

        # ---- records.list with $top + $select ----
        print("\n[1] records.list(LegalEntities, top=3, select=[LegalEntityId, Name])")
        rows = list(
            client.records.list(
                "LegalEntities",
                select=["LegalEntityId", "Name"],
                top=3,
            )
        )
        for r in rows:
            print(f"    -> {r.get('LegalEntityId')!r}  {r.get('Name')!r}")
        print(f"    => yielded {len(rows)} rows (top=3)")

        # ---- records.list paginating across many CustomerGroups ----
        print("\n[2] records.list(CustomerGroups, cross_company=True, page_size=10) iterate first 25")
        cg_iter = client.records.list(
            "CustomerGroups", cross_company=True, page_size=10
        )
        first25 = []
        for row in cg_iter:
            first25.append(row.get("CustomerGroupId"))
            if len(first25) >= 25:
                break
        print(f"    -> first IDs: {first25[:10]} ...")
        print(f"    => collected {len(first25)} rows across pages of 10")

        # ---- metadata.list_data_entities ----
        print("\n[3] metadata.list_data_entities(top=5)  (server ignores $top, SDK caps client-side)")
        de_rows = list(client.metadata.list_data_entities(top=5))
        for r in de_rows:
            print(f"    -> {r.get('Name')}  ({r.get('PublicEntityName')})")
        print(f"    => yielded {len(de_rows)} rows")

        # ---- metadata.get_data_entity for one we know exists ----
        # `Name` is the X++ entity name (e.g. `AbbreviationsEntity`), not the
        # public OData entity set name. We pick whatever name came back from
        # phase 3 above so the smoketest is self-bootstrapping.
        if not de_rows:
            print("\n[4] SKIPPED — phase 3 yielded no rows")
        else:
            target_name = de_rows[0]["Name"]
            print(f"\n[4] metadata.get_data_entity({target_name!r})")
            de = client.metadata.get_data_entity(target_name)
            print(
                f"    -> Name={de.get('Name')!r}  "
                f"PublicEntityName={de.get('PublicEntityName')!r}  "
                f"IsReadOnly={de.get('IsReadOnly')}"
            )

        # ---- typed 404 mapping for missing metadata name ----
        from PowerPlatform.FinOps.errors import FinOpsNotFoundError

        print("\n[5] metadata.get_data_entity('NoSuchEntityXYZ')  (expecting typed 404)")
        try:
            client.metadata.get_data_entity("NoSuchEntityXYZ")
        except FinOpsNotFoundError as exc:
            print(f"    -> typed FinOpsNotFoundError raised: status={exc.status_code}")
        else:  # pragma: no cover
            raise SystemExit("expected FinOpsNotFoundError")

        # ---- metadata.list_public_enumerations small page ----
        print("\n[6] metadata.list_public_enumerations(top=3)")
        for r in client.metadata.list_public_enumerations(top=3):
            print(f"    -> {r.get('Name')}")

    print("\n" + "=" * 72)
    print(" STEP 2 SMOKETEST OK")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
