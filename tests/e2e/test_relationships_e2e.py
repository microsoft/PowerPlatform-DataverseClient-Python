# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
End-to-end relationship tests for the Dataverse SDK.

These tests run against a LIVE Dataverse environment and validate
the full relationship API lifecycle:
- 1:N (one-to-many) relationship CRUD
- N:N (many-to-many) relationship CRUD
- Convenience create_lookup_field API
- Data operations through relationships (@odata.bind, $expand, $filter)
- Cascade behavior verification (RemoveLink, Restrict, Cascade)

Requirements:
- Set DATAVERSE_URL environment variable (e.g. https://yourorg.crm.dynamics.com)
- Azure identity with permissions to create/delete tables and relationships
- Run with: pytest tests/e2e/test_relationships_e2e.py -v -s

These tests are NOT run in CI (they require a live environment).
Mark: @pytest.mark.e2e
"""

import os
import time

import pytest
from azure.identity import InteractiveBrowserCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import HttpError
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    CascadeConfiguration,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel
from PowerPlatform.Dataverse.common.constants import (
    CASCADE_BEHAVIOR_CASCADE,
    CASCADE_BEHAVIOR_NO_CASCADE,
    CASCADE_BEHAVIOR_REMOVE_LINK,
    CASCADE_BEHAVIOR_RESTRICT,
)

pytestmark = pytest.mark.e2e

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _get_org_url():
    """Read DATAVERSE_URL at call time so late-set env vars are picked up."""
    return os.environ.get("DATAVERSE_URL", "")


def _skip_if_no_url():
    if not _get_org_url():
        pytest.skip("DATAVERSE_URL not set -- skipping e2e tests")


@pytest.fixture(scope="module")
def client():
    """Authenticated DataverseClient for the test module."""
    _skip_if_no_url()
    cred = InteractiveBrowserCredential(timeout=600)
    c = DataverseClient(_get_org_url(), cred)
    try:
        yield c
    finally:
        c.close()


def _backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
    """Retry with exponential backoff for metadata propagation."""
    last = None
    for d in delays:
        if d:
            time.sleep(d)
        try:
            return op()
        except Exception as ex:
            last = ex
    raise last


def _wait_for_table(client, schema_name, retries=15, delay=3):
    """Poll until table metadata is available."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            info = client.tables.get(schema_name)
            if info and info.get("entity_set_name"):
                odata = client._get_odata()
                odata._entity_set_from_schema_name(schema_name)
                return info
        except Exception as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(delay)
    msg = f"Table {schema_name} metadata not available after {retries} attempts"
    if last_exc:
        raise RuntimeError(msg) from last_exc
    raise RuntimeError(msg)


def _wait_for_relationship(client, schema_name, retries=15, delay=3):
    """Poll until get_relationship returns a non-None result."""
    for attempt in range(1, retries + 1):
        result = client.tables.get_relationship(schema_name)
        if result is not None:
            return result
        if attempt < retries:
            time.sleep(delay)
    raise RuntimeError(f"Relationship {schema_name} not queryable after {retries} attempts")


def _wait_for_lookup_ready(client, table_schema, lookup_logical, retries=15, delay=3):
    """Poll until a lookup column is queryable on the table."""
    for attempt in range(1, retries + 1):
        try:
            for page in client.records.get(table_schema, select=[lookup_logical], top=1):
                pass  # If we get here without error, the column is ready
            return
        except HttpError:
            pass
        except Exception:
            pass
        if attempt < retries:
            time.sleep(delay)
    raise RuntimeError(f"Lookup column {lookup_logical} not ready on {table_schema} after {retries} attempts")


def _safe_delete_relationship(client, schema_name):
    try:
        rel = client.tables.get_relationship(schema_name)
        if rel and rel.relationship_id:
            client.tables.delete_relationship(rel.relationship_id)
    except Exception:
        pass


def _safe_delete_table(client, schema_name):
    try:
        if client.tables.get(schema_name):
            _backoff(lambda: client.tables.delete(schema_name))
    except Exception:
        pass


def _create_table(client, schema_name, columns=None):
    """Create a table and wait for metadata."""
    cols = columns or {"new_Name": "string"}
    info = _backoff(lambda: client.tables.create(schema_name, cols))
    _wait_for_table(client, schema_name)
    return info


# ---------------------------------------------------------------------------
# Test 1: 1:N Core API -- create, get, delete
# ---------------------------------------------------------------------------


class TestOneToManyCore:
    """One-to-many relationship lifecycle via core API."""

    PARENT = "new_E2E1NPar"
    CHILD = "new_E2E1NChi"
    REL_NAME = "new_E2E1NPar_1NChi"

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, client):
        _safe_delete_relationship(client, self.REL_NAME)
        _safe_delete_table(client, self.CHILD)
        _safe_delete_table(client, self.PARENT)
        yield
        _safe_delete_relationship(client, self.REL_NAME)
        _safe_delete_table(client, self.CHILD)
        _safe_delete_table(client, self.PARENT)

    def test_create_get_delete_1n(self, client):
        """Full 1:N lifecycle: create tables, create relationship, get, delete."""
        parent = _create_table(client, self.PARENT, {"new_Code": "string"})
        child = _create_table(client, self.CHILD, {"new_Num": "string"})

        lookup = LookupAttributeMetadata(
            schema_name="new_ParRef",
            display_name=Label(localized_labels=[LocalizedLabel(label="Parent Ref", language_code=1033)]),
            required_level="None",
        )
        relationship = OneToManyRelationshipMetadata(
            schema_name=self.REL_NAME,
            referenced_entity=parent["table_logical_name"],
            referencing_entity=child["table_logical_name"],
            referenced_attribute=f"{parent['table_logical_name']}id",
            cascade_configuration=CascadeConfiguration(
                delete=CASCADE_BEHAVIOR_REMOVE_LINK,
                assign=CASCADE_BEHAVIOR_NO_CASCADE,
            ),
        )

        result = _backoff(
            lambda: client.tables.create_one_to_many_relationship(lookup=lookup, relationship=relationship)
        )

        # Verify create result
        assert result.relationship_type == "one_to_many"
        assert result.relationship_schema_name == self.REL_NAME
        assert result.relationship_id is not None
        assert result.lookup_schema_name is not None
        assert result.referenced_entity == parent["table_logical_name"]
        assert result.referencing_entity == child["table_logical_name"]
        assert result.entity1_logical_name is None  # N:N only

        # Verify get_relationship
        fetched = _wait_for_relationship(client, self.REL_NAME)
        assert fetched.relationship_type == "one_to_many"
        assert fetched.relationship_id == result.relationship_id
        assert fetched.referenced_entity == parent["table_logical_name"]

        # Verify delete
        client.tables.delete_relationship(result.relationship_id)
        post_delete = client.tables.get_relationship(self.REL_NAME)
        assert post_delete is None


# ---------------------------------------------------------------------------
# Test 2: 1:N Convenience API -- create_lookup_field
# ---------------------------------------------------------------------------


class TestLookupField:
    """Convenience create_lookup_field API."""

    CHILD = "new_E2ELkpChi"

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, client):
        _safe_delete_table(client, self.CHILD)
        self._rel_id = None
        yield
        if self._rel_id:
            try:
                client.tables.delete_relationship(self._rel_id)
            except Exception:
                pass
        _safe_delete_table(client, self.CHILD)

    def test_lookup_to_system_table(self, client):
        """Create lookup from custom table to system 'account'."""
        child = _create_table(client, self.CHILD, {"new_Info": "string"})

        result = _backoff(
            lambda: client.tables.create_lookup_field(
                referencing_table=child["table_logical_name"],
                lookup_field_name="new_AcctLkp",
                referenced_table="account",
                display_name="Account",
                required=False,
                cascade_delete=CASCADE_BEHAVIOR_REMOVE_LINK,
            )
        )
        self._rel_id = result.relationship_id

        assert result.relationship_type == "one_to_many"
        assert result.lookup_schema_name is not None
        assert result.referenced_entity == "account"

        # Verify via get_relationship
        fetched = _wait_for_relationship(client, result.relationship_schema_name)
        assert fetched.referenced_entity == "account"


# ---------------------------------------------------------------------------
# Test 3: N:N -- create, get, delete
# ---------------------------------------------------------------------------


class TestManyToMany:
    """Many-to-many relationship lifecycle."""

    TBL1 = "new_E2ENNTbl1"
    TBL2 = "new_E2ENNTbl2"
    REL_NAME = "new_e2enntbl1_nntbl2"

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, client):
        _safe_delete_relationship(client, self.REL_NAME)
        _safe_delete_table(client, self.TBL1)
        _safe_delete_table(client, self.TBL2)
        yield
        _safe_delete_relationship(client, self.REL_NAME)
        _safe_delete_table(client, self.TBL1)
        _safe_delete_table(client, self.TBL2)

    def test_create_get_delete_nn(self, client):
        """Full N:N lifecycle: create, get, delete."""
        tbl1 = _create_table(client, self.TBL1, {"new_C1": "string"})
        tbl2 = _create_table(client, self.TBL2, {"new_C2": "string"})

        m2m = ManyToManyRelationshipMetadata(
            schema_name=self.REL_NAME,
            entity1_logical_name=tbl1["table_logical_name"],
            entity2_logical_name=tbl2["table_logical_name"],
        )

        result = _backoff(lambda: client.tables.create_many_to_many_relationship(relationship=m2m))

        assert result.relationship_type == "many_to_many"
        assert result.relationship_schema_name == self.REL_NAME
        assert result.entity1_logical_name == tbl1["table_logical_name"]
        assert result.entity2_logical_name == tbl2["table_logical_name"]
        assert result.lookup_schema_name is None  # 1:N only

        # Verify get
        fetched = _wait_for_relationship(client, self.REL_NAME)
        assert fetched.relationship_type == "many_to_many"
        assert fetched.relationship_id == result.relationship_id

        # Verify delete
        client.tables.delete_relationship(result.relationship_id)
        post_delete = client.tables.get_relationship(self.REL_NAME)
        assert post_delete is None

    def test_get_nonexistent_returns_none(self, client):
        """get_relationship returns None for nonexistent relationships."""
        result = client.tables.get_relationship("nonexistent_xyz_relationship_99")
        assert result is None


# ---------------------------------------------------------------------------
# Test 4: Data through relationships -- @odata.bind, $expand, $filter
# ---------------------------------------------------------------------------


class TestDataThroughRelationships:
    """Verify relationships work with actual record operations."""

    PARENT = "new_E2EDataPar"
    CHILD = "new_E2EDataChi"
    REL_NAME = "new_E2EDataPar_DataChi"

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, client):
        _safe_delete_relationship(client, self.REL_NAME)
        _safe_delete_table(client, self.CHILD)
        _safe_delete_table(client, self.PARENT)

        # Create tables + relationship + data
        self.parent_info = _create_table(
            client,
            self.PARENT,
            {
                "new_ParName": "string",
            },
        )
        self.child_info = _create_table(
            client,
            self.CHILD,
            {
                "new_ChiName": "string",
                "new_ChiVal": "int",
            },
        )

        lookup = LookupAttributeMetadata(
            schema_name="new_DataParLkp",
            display_name=Label(localized_labels=[LocalizedLabel(label="Data Parent", language_code=1033)]),
        )
        rel = OneToManyRelationshipMetadata(
            schema_name=self.REL_NAME,
            referenced_entity=self.parent_info["table_logical_name"],
            referencing_entity=self.child_info["table_logical_name"],
            referenced_attribute=f"{self.parent_info['table_logical_name']}id",
            cascade_configuration=CascadeConfiguration(
                delete=CASCADE_BEHAVIOR_REMOVE_LINK,
            ),
        )

        self.rel_result = _backoff(
            lambda: client.tables.create_one_to_many_relationship(lookup=lookup, relationship=rel)
        )

        # Get server-assigned navigation property name
        rel_info = _wait_for_relationship(client, self.REL_NAME)
        self.server_nav_prop = rel_info.lookup_schema_name
        self.lookup_value_key = f"_{self.server_nav_prop.lower()}_value"

        # Wait for lookup column to become queryable (instead of hard-coded sleep)
        _wait_for_lookup_ready(client, self.CHILD, self.lookup_value_key)

        # Get entity set for @odata.bind
        parent_full = client.tables.get(self.PARENT)
        self.entity_set = parent_full["entity_set_name"]

        # Create parent records
        self.p1_id = _backoff(lambda: client.records.create(self.PARENT, {"new_parname": "Alpha Corp"}))
        self.p2_id = _backoff(lambda: client.records.create(self.PARENT, {"new_parname": "Beta Inc"}))

        # Create child records -- use server_nav_prop for @odata.bind
        # (server-assigned nav prop is authoritative for case-sensitive OData operations)
        nav = self.server_nav_prop
        es = self.entity_set
        p1 = self.p1_id
        p2 = self.p2_id

        self.c1_id = _backoff(
            lambda: client.records.create(
                self.CHILD,
                {
                    "new_chiname": "Child A1",
                    "new_chival": 100,
                    f"{nav}@odata.bind": f"/{es}({p1})",
                },
            )
        )
        self.c2_id = _backoff(
            lambda: client.records.create(
                self.CHILD,
                {
                    "new_chiname": "Child A2",
                    "new_chival": 200,
                    f"{nav}@odata.bind": f"/{es}({p1})",
                },
            )
        )
        self.c3_id = _backoff(
            lambda: client.records.create(
                self.CHILD,
                {
                    "new_chiname": "Child B1",
                    "new_chival": 300,
                    f"{nav}@odata.bind": f"/{es}({p2})",
                },
            )
        )
        self.c4_id = _backoff(
            lambda: client.records.create(
                self.CHILD,
                {
                    "new_chiname": "Orphan",
                    "new_chival": 0,
                },
            )
        )

        yield

        # Cleanup
        for cid in [self.c1_id, self.c2_id, self.c3_id, self.c4_id]:
            try:
                client.records.delete(self.CHILD, cid)
            except Exception:
                pass
        for pid in [self.p1_id, self.p2_id]:
            try:
                client.records.delete(self.PARENT, pid)
            except Exception:
                pass
        try:
            client.tables.delete_relationship(self.rel_result.relationship_id)
        except Exception:
            pass
        _safe_delete_table(client, self.CHILD)
        _safe_delete_table(client, self.PARENT)

    def test_odata_bind_creates_lookup(self, client):
        """Records created with @odata.bind have correct lookup values."""
        c1 = client.records.get(self.CHILD, self.c1_id)
        assert c1.get(self.lookup_value_key) is not None
        assert c1[self.lookup_value_key].lower() == self.p1_id.lower()

        c4 = client.records.get(self.CHILD, self.c4_id)
        assert c4.get(self.lookup_value_key) is None

    def test_expand_returns_parent_data(self, client):
        """$expand on navigation property returns parent fields."""
        all_recs = []
        for page in client.records.get(
            self.CHILD,
            select=["new_chiname"],
            expand=[self.server_nav_prop],
            top=10,
        ):
            all_recs.extend(page)

        assert len(all_recs) >= 4
        bound = [r for r in all_recs if r.get(self.server_nav_prop) is not None]
        assert len(bound) >= 3
        for rec in bound:
            assert rec[self.server_nav_prop].get("new_parname") is not None

    def test_filter_on_lookup_value(self, client):
        """$filter on lookup _value returns correct children."""
        filtered = []
        for page in client.records.get(
            self.CHILD,
            select=["new_chiname"],
            filter=f"{self.lookup_value_key} eq {self.p1_id}",
            top=10,
        ):
            filtered.extend(page)

        assert len(filtered) == 2
        names = {r["new_chiname"] for r in filtered}
        assert names == {"Child A1", "Child A2"}

    def test_update_lookup_binding(self, client):
        """Updating @odata.bind changes lookup value."""
        client.records.update(
            self.CHILD,
            self.c1_id,
            {
                f"{self.server_nav_prop}@odata.bind": f"/{self.entity_set}({self.p2_id})",
            },
        )
        updated = client.records.get(self.CHILD, self.c1_id)
        assert updated[self.lookup_value_key].lower() == self.p2_id.lower()

        # Restore
        client.records.update(
            self.CHILD,
            self.c1_id,
            {
                f"{self.server_nav_prop}@odata.bind": f"/{self.entity_set}({self.p1_id})",
            },
        )


# ---------------------------------------------------------------------------
# Test 5: Cascade behaviors
# ---------------------------------------------------------------------------


class TestCascadeBehaviors:
    """Verify cascade configuration affects data operations."""

    def _create_cascade_setup(self, client, parent_schema, child_schema, rel_name, lookup_name, cascade_delete):
        """Create parent/child with specified cascade and return IDs."""
        _safe_delete_relationship(client, rel_name)
        _safe_delete_table(client, child_schema)
        _safe_delete_table(client, parent_schema)

        parent = _create_table(client, parent_schema, {"new_Name": "string"})
        child = _create_table(client, child_schema, {"new_Info": "string"})

        lookup = LookupAttributeMetadata(
            schema_name=lookup_name,
            display_name=Label(localized_labels=[LocalizedLabel(label="Cascade", language_code=1033)]),
        )
        relationship = OneToManyRelationshipMetadata(
            schema_name=rel_name,
            referenced_entity=parent["table_logical_name"],
            referencing_entity=child["table_logical_name"],
            referenced_attribute=f"{parent['table_logical_name']}id",
            cascade_configuration=CascadeConfiguration(
                delete=cascade_delete,
                assign=CASCADE_BEHAVIOR_NO_CASCADE,
            ),
        )

        rel_result = _backoff(
            lambda: client.tables.create_one_to_many_relationship(lookup=lookup, relationship=relationship)
        )
        rel_info = _wait_for_relationship(client, rel_name)

        parent_full = client.tables.get(parent_schema)
        entity_set = parent_full["entity_set_name"]
        nav_prop = rel_info.lookup_schema_name

        # Wait for lookup column to become queryable
        lookup_value_key = f"_{nav_prop.lower()}_value"
        _wait_for_lookup_ready(client, child_schema, lookup_value_key)

        return rel_result, entity_set, nav_prop

    def test_restrict_prevents_parent_delete(self, client):
        """Restrict cascade: deleting parent with children fails."""
        ps, cs, rn = "new_E2ERestrPar", "new_E2ERestrChi", "new_E2ERestrPar_RestrChi"
        rel_result = None
        p_id = c_id = None
        try:
            rel_result, entity_set, nav_prop = self._create_cascade_setup(
                client, ps, cs, rn, "new_RestrRef", CASCADE_BEHAVIOR_RESTRICT
            )

            p_id = _backoff(lambda: client.records.create(ps, {"new_name": "Restrict Parent"}))
            c_id = _backoff(
                lambda: client.records.create(
                    cs,
                    {
                        "new_info": "Restrict Child",
                        f"{nav_prop}@odata.bind": f"/{entity_set}({p_id})",
                    },
                )
            )

            # Delete parent should fail
            with pytest.raises(HttpError):
                client.records.delete(ps, p_id)

            # Both records still exist
            assert client.records.get(ps, p_id) is not None
            assert client.records.get(cs, c_id) is not None

            # Remove child, then parent delete succeeds
            client.records.delete(cs, c_id)
            c_id = None
            client.records.delete(ps, p_id)
            p_id = None

        finally:
            if c_id:
                try:
                    client.records.delete(cs, c_id)
                except Exception:
                    pass
            if p_id:
                try:
                    client.records.delete(ps, p_id)
                except Exception:
                    pass
            if rel_result:
                try:
                    client.tables.delete_relationship(rel_result.relationship_id)
                except Exception:
                    pass
            _safe_delete_table(client, cs)
            _safe_delete_table(client, ps)

    def test_cascade_deletes_children(self, client):
        """Cascade delete: deleting parent also deletes children."""
        ps, cs, rn = "new_E2ECascPar", "new_E2ECascChi", "new_E2ECascPar_CascChi"
        rel_result = None
        try:
            rel_result, entity_set, nav_prop = self._create_cascade_setup(
                client, ps, cs, rn, "new_CascRef", CASCADE_BEHAVIOR_CASCADE
            )

            p_id = _backoff(lambda: client.records.create(ps, {"new_name": "Cascade Parent"}))
            c1_id = _backoff(
                lambda: client.records.create(
                    cs,
                    {
                        "new_info": "Cascade Child 1",
                        f"{nav_prop}@odata.bind": f"/{entity_set}({p_id})",
                    },
                )
            )
            c2_id = _backoff(
                lambda: client.records.create(
                    cs,
                    {
                        "new_info": "Cascade Child 2",
                        f"{nav_prop}@odata.bind": f"/{entity_set}({p_id})",
                    },
                )
            )

            # Delete parent -- children should be cascade-deleted
            client.records.delete(ps, p_id)

            # Poll until children return 404 (cascade may be async)
            for cid in [c1_id, c2_id]:
                for attempt in range(1, 11):
                    try:
                        client.records.get(cs, cid)
                    except HttpError as e:
                        if e.status_code == 404:
                            break
                        raise
                    if attempt < 10:
                        time.sleep(2)
                else:
                    pytest.fail(f"Child {cid} still exists after cascade delete")

        finally:
            if rel_result:
                try:
                    client.tables.delete_relationship(rel_result.relationship_id)
                except Exception:
                    pass
            _safe_delete_table(client, cs)
            _safe_delete_table(client, ps)


# ---------------------------------------------------------------------------
# Test 6: Type detection -- get_relationship distinguishes 1:N vs N:N
# ---------------------------------------------------------------------------


class TestTypeDetection:
    """get_relationship returns correct type for both relationship kinds."""

    TBL1 = "new_E2ETypeTbl1"
    TBL2 = "new_E2ETypeTbl2"
    REL_1N = "new_E2ETypeTbl1_TypeTbl2"
    REL_NN = "new_e2etypetbl1_typetbl2_nn"

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, client):
        for r in [self.REL_1N, self.REL_NN]:
            _safe_delete_relationship(client, r)
        for t in [self.TBL1, self.TBL2]:
            _safe_delete_table(client, t)
        self._rel_ids = []
        yield
        for rid in self._rel_ids:
            try:
                client.tables.delete_relationship(rid)
            except Exception:
                pass
        for t in [self.TBL1, self.TBL2]:
            _safe_delete_table(client, t)

    def test_type_detection(self, client):
        """get_relationship correctly detects 1:N vs N:N."""
        tbl1 = _create_table(client, self.TBL1, {"new_TC1": "string"})
        tbl2 = _create_table(client, self.TBL2, {"new_TC2": "string"})

        # Create 1:N
        lookup = LookupAttributeMetadata(
            schema_name="new_TypeRef",
            display_name=Label(localized_labels=[LocalizedLabel(label="Type Test", language_code=1033)]),
        )
        result_1n = _backoff(
            lambda: client.tables.create_one_to_many_relationship(
                lookup=lookup,
                relationship=OneToManyRelationshipMetadata(
                    schema_name=self.REL_1N,
                    referenced_entity=tbl1["table_logical_name"],
                    referencing_entity=tbl2["table_logical_name"],
                    referenced_attribute=f"{tbl1['table_logical_name']}id",
                ),
            )
        )
        self._rel_ids.append(result_1n.relationship_id)

        # Create N:N
        result_nn = _backoff(
            lambda: client.tables.create_many_to_many_relationship(
                relationship=ManyToManyRelationshipMetadata(
                    schema_name=self.REL_NN,
                    entity1_logical_name=tbl1["table_logical_name"],
                    entity2_logical_name=tbl2["table_logical_name"],
                ),
            )
        )
        self._rel_ids.append(result_nn.relationship_id)

        # Verify type detection
        fetched_1n = _wait_for_relationship(client, self.REL_1N)
        assert fetched_1n.relationship_type == "one_to_many"
        assert fetched_1n.referenced_entity is not None
        assert fetched_1n.entity1_logical_name is None

        fetched_nn = _wait_for_relationship(client, self.REL_NN)
        assert fetched_nn.relationship_type == "many_to_many"
        assert fetched_nn.entity1_logical_name is not None
        assert fetched_nn.lookup_schema_name is None
