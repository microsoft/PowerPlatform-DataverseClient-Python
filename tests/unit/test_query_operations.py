# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.operations.query import QueryOperations


class TestQueryOperations(unittest.TestCase):
    """Unit tests for the client.query namespace (QueryOperations)."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    # ---------------------------------------------------------------- namespace

    def test_namespace_exists(self):
        """The client.query attribute should be a QueryOperations instance."""
        self.assertIsInstance(self.client.query, QueryOperations)

    # -------------------------------------------------------------------- sql

    def test_sql(self):
        """sql() should return Record objects with dict-like access."""
        raw_rows = [
            {"accountid": "1", "name": "Contoso"},
            {"accountid": "2", "name": "Fabrikam"},
        ]
        self.client._odata._query_sql.return_value = raw_rows

        result = self.client.query.sql("SELECT accountid, name FROM account")

        self.client._odata._query_sql.assert_called_once_with("SELECT accountid, name FROM account")
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], Record)
        self.assertEqual(result[0]["name"], "Contoso")
        self.assertEqual(result[1]["name"], "Fabrikam")

    def test_sql_empty_result(self):
        """sql() should return an empty list when _query_sql returns no rows."""
        self.client._odata._query_sql.return_value = []

        result = self.client.query.sql("SELECT name FROM account WHERE name = 'NonExistent'")

        self.client._odata._query_sql.assert_called_once_with("SELECT name FROM account WHERE name = 'NonExistent'")
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])

    def test_sql_join(self):
        """sql() should handle JOIN SQL and return Record objects."""
        raw_rows = [
            {"name": "Contoso", "fullname": "John Doe"},
            {"name": "Fabrikam", "fullname": "Jane Smith"},
        ]
        self.client._odata._query_sql.return_value = raw_rows

        result = self.client.query.sql(
            "SELECT a.name, c.fullname FROM account a " "JOIN contact c ON a.accountid = c.parentcustomerid"
        )

        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], Record)
        self.assertEqual(result[0]["name"], "Contoso")
        self.assertEqual(result[0]["fullname"], "John Doe")

    def test_sql_aggregate(self):
        """sql() should handle aggregate results."""
        self.client._odata._query_sql.return_value = [{"cnt": 42}]

        result = self.client.query.sql("SELECT COUNT(*) as cnt FROM account")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["cnt"], 42)

    def test_sql_group_by(self):
        """sql() should handle GROUP BY results."""
        raw = [
            {"statecode": 0, "cnt": 100},
            {"statecode": 1, "cnt": 5},
        ]
        self.client._odata._query_sql.return_value = raw

        result = self.client.query.sql("SELECT statecode, COUNT(*) as cnt FROM account GROUP BY statecode")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["statecode"], 0)
        self.assertEqual(result[0]["cnt"], 100)

    def test_sql_distinct(self):
        """sql() should handle DISTINCT results."""
        raw = [{"name": "Contoso"}, {"name": "Fabrikam"}]
        self.client._odata._query_sql.return_value = raw

        result = self.client.query.sql("SELECT DISTINCT name FROM account")

        self.assertEqual(len(result), 2)

    def test_sql_polymorphic_owner_join(self):
        """sql() should handle polymorphic lookup JOINs (ownerid -> systemuser)."""
        raw = [
            {"name": "Contoso", "owner_name": "Admin User"},
        ]
        self.client._odata._query_sql.return_value = raw

        result = self.client.query.sql(
            "SELECT a.name, su.fullname as owner_name "
            "FROM account a "
            "JOIN systemuser su ON a._ownerid_value = su.systemuserid"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["owner_name"], "Admin User")

    def test_sql_audit_trail_multi_join(self):
        """sql() should handle multi-JOIN for audit trail (createdby + modifiedby)."""
        raw = [
            {"name": "Contoso", "created_by": "User A", "modified_by": "User B"},
        ]
        self.client._odata._query_sql.return_value = raw

        result = self.client.query.sql(
            "SELECT a.name, creator.fullname as created_by, modifier.fullname as modified_by "
            "FROM account a "
            "JOIN systemuser creator ON a._createdby_value = creator.systemuserid "
            "JOIN systemuser modifier ON a._modifiedby_value = modifier.systemuserid"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["created_by"], "User A")

    def test_sql_offset_fetch(self):
        """sql() should handle OFFSET FETCH pagination SQL."""
        raw = [{"name": "Page2-Row1"}]
        self.client._odata._query_sql.return_value = raw

        result = self.client.query.sql("SELECT name FROM account ORDER BY name OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY")

        self.assertEqual(len(result), 1)
        self.client._odata._query_sql.assert_called_once()

    def test_sql_select_star_raises_validation_error(self):
        """sql() must propagate ValidationError when SELECT * is used.

        SELECT * is intentionally rejected -- not a technical limitation but
        a deliberate design decision to prevent expensive wildcard queries on
        wide entities.  The guardrail fires inside _query_sql and the
        ValidationError bubbles up through query.sql() unchanged.
        """
        from PowerPlatform.Dataverse.core.errors import ValidationError

        self.client._odata._query_sql.side_effect = ValidationError(
            "SELECT * is not supported.",
            subcode="validation_sql_unsupported_syntax",
        )
        with self.assertRaises(ValidationError):
            self.client.query.sql("SELECT * FROM account")

    # ----------------------------------------------------------------- builder

    def test_builder_returns_query_builder(self):
        """builder() should return a QueryBuilder with _query_ops set."""
        from PowerPlatform.Dataverse.models.query_builder import QueryBuilder

        qb = self.client.query.builder("account")

        self.assertIsInstance(qb, QueryBuilder)
        self.assertEqual(qb.table, "account")
        self.assertIs(qb._query_ops, self.client.query)

    def test_builder_execute_flat_default(self):
        """builder().execute() should return flat records by default."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1", "name": "Test"}]])

        records = list(self.client.query.builder("account").select("name").filter_eq("statecode", 0).top(10).execute())

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=None,
            top=10,
            expand=None,
            page_size=None,
            count=False,
            include_annotations=None,
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "Test")

    def test_builder_execute_flat_multiple_pages(self):
        """execute() should flatten records from multiple pages."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}], [{"accountid": "2"}]])

        records = list(self.client.query.builder("account").select("name").execute())

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["accountid"], "1")
        self.assertEqual(records[1]["accountid"], "2")

    def test_builder_execute_by_page(self):
        """execute(by_page=True) should yield pages."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}], [{"accountid": "2"}]])

        pages = list(self.client.query.builder("account").select("name").execute(by_page=True))

        self.assertEqual(len(pages), 2)
        self.assertEqual(len(pages[0]), 1)
        self.assertEqual(pages[0][0]["accountid"], "1")
        self.assertEqual(pages[1][0]["accountid"], "2")

    def test_builder_execute_all_params(self):
        """builder().execute() should forward all parameters."""
        self.client._odata._get_multiple.return_value = iter([[{"name": "Test"}]])

        list(
            self.client.query.builder("account")
            .select("name", "revenue")
            .filter_eq("statecode", 0)
            .filter_gt("revenue", 1000000)
            .order_by("revenue", descending=True)
            .expand("primarycontactid")
            .top(50)
            .page_size(25)
            .execute()
        )

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name", "revenue"],
            filter="statecode eq 0 and revenue gt 1000000",
            orderby=["revenue desc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
            count=False,
            include_annotations=None,
        )

    def test_builder_execute_with_where(self):
        """builder().where().execute() should compile expression to filter."""
        from PowerPlatform.Dataverse.models.filters import eq, gt

        self.client._odata._get_multiple.return_value = iter([[{"name": "Test"}]])

        list(
            self.client.query.builder("account")
            .where((eq("statecode", 0) | eq("statecode", 1)) & gt("revenue", 100000))
            .execute()
        )

        call_kwargs = self.client._odata._get_multiple.call_args
        self.assertEqual(
            call_kwargs.kwargs["filter"],
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )

    def test_builder_execute_with_filter_in(self):
        """builder().filter_in().execute() should forward CRM.In filter to _get_multiple."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}]])

        list(self.client.query.builder("account").select("name").filter_in("statecode", [0, 1, 2]).execute())

        call_kwargs = self.client._odata._get_multiple.call_args
        self.assertEqual(
            call_kwargs.kwargs["filter"],
            'Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1","2"])',
        )

    def test_builder_execute_with_where_filter_in(self):
        """builder().where(filter_in(...) & ...).execute() should compile composed expression."""
        from PowerPlatform.Dataverse.models.filters import filter_in, gt

        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}]])

        list(
            self.client.query.builder("account").where(filter_in("statecode", [0, 1]) & gt("revenue", 100000)).execute()
        )

        call_kwargs = self.client._odata._get_multiple.call_args
        self.assertEqual(
            call_kwargs.kwargs["filter"],
            '(Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1"]) and revenue gt 100000)',
        )

    def test_builder_execute_with_filter_between_datetimes(self):
        """builder().filter_between() with datetimes should forward correct OData."""
        from datetime import datetime, timezone

        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}]])

        start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        list(self.client.query.builder("account").filter_between("createdon", start, end).execute())

        call_kwargs = self.client._odata._get_multiple.call_args
        self.assertEqual(
            call_kwargs.kwargs["filter"],
            "(createdon ge 2024-01-01T00:00:00Z and createdon le 2024-12-31T23:59:59Z)",
        )

    def test_builder_execute_with_filter_not_in(self):
        """builder().filter_not_in().execute() should forward CRM.NotIn filter."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}]])

        list(self.client.query.builder("account").select("name").filter_not_in("statecode", [2, 3]).execute())

        call_kwargs = self.client._odata._get_multiple.call_args
        self.assertEqual(
            call_kwargs.kwargs["filter"],
            'Microsoft.Dynamics.CRM.NotIn(PropertyName=\'statecode\',PropertyValues=["2","3"])',
        )

    def test_builder_execute_with_filter_not_between(self):
        """builder().filter_not_between().execute() should forward negated between filter."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}]])

        list(self.client.query.builder("account").filter_not_between("revenue", 100000, 500000).execute())

        call_kwargs = self.client._odata._get_multiple.call_args
        self.assertEqual(
            call_kwargs.kwargs["filter"],
            "not ((revenue ge 100000 and revenue le 500000))",
        )

    def test_builder_full_fluent_workflow(self):
        """End-to-end test of the fluent query workflow."""
        expected_records = [
            {"accountid": "1", "name": "Big Corp", "revenue": 5000000},
            {"accountid": "2", "name": "Mega Inc", "revenue": 4000000},
        ]
        self.client._odata._get_multiple.return_value = iter([expected_records])

        records = list(
            self.client.query.builder("account")
            .select("name", "revenue")
            .filter_eq("statecode", 0)
            .filter_gt("revenue", 1000000)
            .order_by("revenue", descending=True)
            .expand("primarycontactid")
            .top(10)
            .page_size(5)
            .execute()
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["name"], "Big Corp")
        self.assertEqual(records[1]["name"], "Mega Inc")

    def test_builder_to_dataframe(self):
        """builder().to_dataframe() should delegate to client.dataframe.get()."""
        import pandas as pd

        expected_df = pd.DataFrame([{"name": "Contoso", "revenue": 1000}])
        self.client.dataframe = MagicMock()
        self.client.dataframe.get.return_value = expected_df

        result = (
            self.client.query.builder("account")
            .select("name", "revenue")
            .filter_eq("statecode", 0)
            .order_by("name")
            .top(50)
            .to_dataframe()
        )

        self.client.dataframe.get.assert_called_once_with(
            "account",
            select=["name", "revenue"],
            filter="statecode eq 0",
            orderby=["name"],
            top=50,
            expand=None,
            page_size=None,
            count=False,
            include_annotations=None,
        )
        pd.testing.assert_frame_equal(result, expected_df)


# ===================================================================
# SQL Helper Tests
# ===================================================================


class TestSqlColumns(unittest.TestCase):
    """Tests for client.query.sql_columns()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def _mock_columns(self, columns):
        self.client._odata._list_columns.return_value = columns

    def test_basic_columns(self):
        self._mock_columns(
            [
                {
                    "LogicalName": "accountid",
                    "AttributeType": "Uniqueidentifier",
                    "IsPrimaryId": True,
                    "IsPrimaryName": False,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Account"}},
                },
                {
                    "LogicalName": "name",
                    "AttributeType": "String",
                    "IsPrimaryId": False,
                    "IsPrimaryName": True,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Account Name"}},
                },
                {
                    "LogicalName": "revenue",
                    "AttributeType": "Money",
                    "IsPrimaryId": False,
                    "IsPrimaryName": False,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Annual Revenue"}},
                },
            ]
        )
        cols = self.client.query.sql_columns("account")
        self.assertEqual(len(cols), 3)
        # PK first, then name, then alphabetical
        self.assertEqual(cols[0]["name"], "accountid")
        self.assertTrue(cols[0]["is_pk"])
        self.assertEqual(cols[1]["name"], "name")
        self.assertTrue(cols[1]["is_name"])
        self.assertEqual(cols[2]["name"], "revenue")
        self.assertEqual(cols[2]["label"], "Annual Revenue")

    def test_excludes_system_columns(self):
        self._mock_columns(
            [
                {
                    "LogicalName": "name",
                    "AttributeType": "String",
                    "IsPrimaryId": False,
                    "IsPrimaryName": True,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Name"}},
                },
                {
                    "LogicalName": "revenue_base",
                    "AttributeType": "Money",
                    "IsPrimaryId": False,
                    "IsPrimaryName": False,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Revenue Base"}},
                },
                {
                    "LogicalName": "versionnumber",
                    "AttributeType": "BigInt",
                    "IsPrimaryId": False,
                    "IsPrimaryName": False,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Version"}},
                },
            ]
        )
        cols = self.client.query.sql_columns("account", include_system=False)
        names = [c["name"] for c in cols]
        self.assertIn("name", names)
        self.assertNotIn("revenue_base", names)
        self.assertNotIn("versionnumber", names)

    def test_include_system_columns(self):
        self._mock_columns(
            [
                {
                    "LogicalName": "name",
                    "AttributeType": "String",
                    "IsPrimaryId": False,
                    "IsPrimaryName": False,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Name"}},
                },
                {
                    "LogicalName": "versionnumber",
                    "AttributeType": "BigInt",
                    "IsPrimaryId": False,
                    "IsPrimaryName": False,
                    "DisplayName": {"UserLocalizedLabel": {"Label": "Version"}},
                },
            ]
        )
        cols = self.client.query.sql_columns("account", include_system=True)
        names = [c["name"] for c in cols]
        self.assertIn("versionnumber", names)

    def test_empty_table(self):
        self._mock_columns([])
        cols = self.client.query.sql_columns("account")
        self.assertEqual(cols, [])

    def test_excludes_attribute_of_columns(self):
        """Columns with AttributeOf set (computed display names) should be excluded."""
        self._mock_columns(
            [
                {
                    "LogicalName": "name",
                    "AttributeType": "String",
                    "IsPrimaryId": False,
                    "IsPrimaryName": True,
                    "DisplayName": {},
                },
                {
                    "LogicalName": "createdbyname",
                    "AttributeType": "String",
                    "IsPrimaryId": False,
                    "IsPrimaryName": False,
                    "DisplayName": {},
                    "AttributeOf": "createdby",
                },
            ]
        )
        cols = self.client.query.sql_columns("account")
        names = [c["name"] for c in cols]
        self.assertIn("name", names)
        self.assertNotIn("createdbyname", names)


class TestSqlSelect(unittest.TestCase):
    """Tests for client.query.sql_select()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_returns_comma_separated(self):
        self.client._odata._list_columns.return_value = [
            {
                "LogicalName": "accountid",
                "AttributeType": "Uniqueidentifier",
                "IsPrimaryId": True,
                "IsPrimaryName": False,
                "DisplayName": {},
            },
            {
                "LogicalName": "name",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": True,
                "DisplayName": {},
            },
            {
                "LogicalName": "revenue",
                "AttributeType": "Money",
                "IsPrimaryId": False,
                "IsPrimaryName": False,
                "DisplayName": {},
            },
        ]
        result = self.client.query.sql_select("account")
        self.assertIn("accountid", result)
        self.assertIn("name", result)
        self.assertIn("revenue", result)
        self.assertEqual(result.count(","), 2)  # 3 cols = 2 commas


class TestSqlJoins(unittest.TestCase):
    """Tests for client.query.sql_joins()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def _mock_rels(self, rels):
        self.client._odata._list_table_relationships.return_value = rels

    def test_outgoing_lookups(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "contact",
                    "ReferencingAttribute": "parentcustomerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "SchemaName": "contact_customer_accounts",
                },
            ]
        )
        joins = self.client.query.sql_joins("contact")
        self.assertEqual(len(joins), 1)
        j = joins[0]
        self.assertEqual(j["column"], "parentcustomerid")
        self.assertEqual(j["target"], "account")
        self.assertEqual(j["target_pk"], "accountid")
        self.assertIn("JOIN account", j["join_clause"])
        self.assertIn("parentcustomerid", j["join_clause"])

    def test_ignores_incoming_rels(self):
        self._mock_rels(
            [
                # This is an incoming relationship (account is referenced, not referencing)
                {
                    "ReferencingEntity": "opportunity",
                    "ReferencingAttribute": "customerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "SchemaName": "opp_customer_accounts",
                },
            ]
        )
        joins = self.client.query.sql_joins("account")
        self.assertEqual(len(joins), 0)

    def test_polymorphic_returns_multiple(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "opportunity",
                    "ReferencingAttribute": "customerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "SchemaName": "opp_customer_accounts",
                },
                {
                    "ReferencingEntity": "opportunity",
                    "ReferencingAttribute": "customerid",
                    "ReferencedEntity": "contact",
                    "ReferencedAttribute": "contactid",
                    "SchemaName": "opp_customer_contacts",
                },
            ]
        )
        joins = self.client.query.sql_joins("opportunity")
        self.assertEqual(len(joins), 2)
        targets = {j["target"] for j in joins}
        self.assertEqual(targets, {"account", "contact"})
        # Both use the same source column
        self.assertTrue(all(j["column"] == "customerid" for j in joins))

    def test_empty_relationships(self):
        self._mock_rels([])
        joins = self.client.query.sql_joins("account")
        self.assertEqual(joins, [])


class TestSqlJoin(unittest.TestCase):
    """Tests for client.query.sql_join()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def _mock_rels(self, rels):
        self.client._odata._list_table_relationships.return_value = rels

    def test_generates_join_clause(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "contact",
                    "ReferencingAttribute": "parentcustomerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "SchemaName": "contact_customer_accounts",
                },
            ]
        )
        result = self.client.query.sql_join("contact", "account", from_alias="c", to_alias="a")
        self.assertEqual(result, "JOIN account a ON c.parentcustomerid = a.accountid")

    def test_default_aliases(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "contact",
                    "ReferencingAttribute": "parentcustomerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "SchemaName": "contact_customer_accounts",
                },
            ]
        )
        result = self.client.query.sql_join("contact", "account")
        self.assertIn("JOIN account a ON contact.parentcustomerid = a.accountid", result)

    def test_no_relationship_raises(self):
        self._mock_rels([])
        with self.assertRaises(ValueError) as ctx:
            self.client.query.sql_join("contact", "nonexistent")
        self.assertIn("No relationship found", str(ctx.exception))

    def test_case_insensitive_target(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "contact",
                    "ReferencingAttribute": "ownerid",
                    "ReferencedEntity": "systemuser",
                    "ReferencedAttribute": "systemuserid",
                    "SchemaName": "contact_owner",
                },
            ]
        )
        result = self.client.query.sql_join("contact", "SystemUser", from_alias="c", to_alias="su")
        self.assertIn("JOIN systemuser su", result)
        self.assertIn("c.ownerid = su.systemuserid", result)


# ===================================================================
# OData Helper Tests
# ===================================================================


class TestOdataSelect(unittest.TestCase):
    """Tests for client.query.odata_select()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_returns_list_of_strings(self):
        self.client._odata._list_columns.return_value = [
            {
                "LogicalName": "accountid",
                "AttributeType": "Uniqueidentifier",
                "IsPrimaryId": True,
                "IsPrimaryName": False,
                "DisplayName": {},
            },
            {
                "LogicalName": "name",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": True,
                "DisplayName": {},
            },
        ]
        result = self.client.query.odata_select("account")
        self.assertIsInstance(result, list)
        self.assertIn("accountid", result)
        self.assertIn("name", result)

    def test_result_usable_in_records_get(self):
        """odata_select returns list that matches records.get(select=) format."""
        self.client._odata._list_columns.return_value = [
            {
                "LogicalName": "name",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": True,
                "DisplayName": {},
            },
        ]
        cols = self.client.query.odata_select("account")
        self.assertEqual(cols, ["name"])


class TestOdataExpands(unittest.TestCase):
    """Tests for client.query.odata_expands()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def _mock_rels(self, rels):
        self.client._odata._list_table_relationships.return_value = rels

    def _mock_entity_set(self, name):
        self.client._odata._entity_set_from_schema_name.return_value = name

    def test_outgoing_lookups(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "contact",
                    "ReferencingAttribute": "parentcustomerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "ReferencingEntityNavigationPropertyName": "parentcustomerid_account",
                    "SchemaName": "contact_customer_accounts",
                },
            ]
        )
        self._mock_entity_set("accounts")
        expands = self.client.query.odata_expands("contact")
        self.assertEqual(len(expands), 1)
        e = expands[0]
        self.assertEqual(e["nav_property"], "parentcustomerid_account")
        self.assertEqual(e["target_table"], "account")
        self.assertEqual(e["target_entity_set"], "accounts")
        self.assertEqual(e["lookup_attribute"], "parentcustomerid")

    def test_ignores_incoming_rels(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "opportunity",
                    "ReferencingAttribute": "customerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "ReferencingEntityNavigationPropertyName": "customerid_account",
                    "SchemaName": "opp_customer",
                },
            ]
        )
        expands = self.client.query.odata_expands("account")
        self.assertEqual(len(expands), 0)

    def test_polymorphic_returns_multiple(self):
        self._mock_rels(
            [
                {
                    "ReferencingEntity": "opportunity",
                    "ReferencingAttribute": "customerid",
                    "ReferencedEntity": "account",
                    "ReferencedAttribute": "accountid",
                    "ReferencingEntityNavigationPropertyName": "customerid_account",
                    "SchemaName": "opp_customer_accounts",
                },
                {
                    "ReferencingEntity": "opportunity",
                    "ReferencingAttribute": "customerid",
                    "ReferencedEntity": "contact",
                    "ReferencedAttribute": "contactid",
                    "ReferencingEntityNavigationPropertyName": "customerid_contact",
                    "SchemaName": "opp_customer_contacts",
                },
            ]
        )
        self._mock_entity_set("accounts")
        expands = self.client.query.odata_expands("opportunity")
        self.assertEqual(len(expands), 2)
        nav_props = {e["nav_property"] for e in expands}
        self.assertEqual(nav_props, {"customerid_account", "customerid_contact"})


class TestOdataExpand(unittest.TestCase):
    """Tests for client.query.odata_expand()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_returns_nav_property(self):
        self.client._odata._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "contact",
                "ReferencingAttribute": "parentcustomerid",
                "ReferencedEntity": "account",
                "ReferencedAttribute": "accountid",
                "ReferencingEntityNavigationPropertyName": "parentcustomerid_account",
                "SchemaName": "contact_customer_accounts",
            },
        ]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"
        result = self.client.query.odata_expand("contact", "account")
        self.assertEqual(result, "parentcustomerid_account")

    def test_no_relationship_raises(self):
        self.client._odata._list_table_relationships.return_value = []
        with self.assertRaises(ValueError) as ctx:
            self.client.query.odata_expand("contact", "nonexistent")
        self.assertIn("No navigation property", str(ctx.exception))

    def test_case_insensitive_target(self):
        self.client._odata._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "contact",
                "ReferencingAttribute": "ownerid",
                "ReferencedEntity": "systemuser",
                "ReferencedAttribute": "systemuserid",
                "ReferencingEntityNavigationPropertyName": "ownerid_systemuser",
                "SchemaName": "contact_owner",
            },
        ]
        self.client._odata._entity_set_from_schema_name.return_value = "systemusers"
        result = self.client.query.odata_expand("contact", "SystemUser")
        self.assertEqual(result, "ownerid_systemuser")


class TestOdataBind(unittest.TestCase):
    """Tests for client.query.odata_bind()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_returns_bind_dict(self):
        self.client._odata._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "contact",
                "ReferencingAttribute": "parentcustomerid",
                "ReferencedEntity": "account",
                "ReferencedAttribute": "accountid",
                "ReferencingEntityNavigationPropertyName": "parentcustomerid_account",
                "SchemaName": "contact_customer_accounts",
            },
        ]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        guid = "12345678-1234-1234-1234-123456789abc"
        result = self.client.query.odata_bind("contact", "account", guid)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 1)
        key = list(result.keys())[0]
        self.assertEqual(key, "parentcustomerid_account@odata.bind")
        self.assertEqual(result[key], f"/accounts({guid})")

    def test_no_relationship_raises(self):
        self.client._odata._list_table_relationships.return_value = []
        with self.assertRaises(ValueError):
            self.client.query.odata_bind("contact", "nonexistent", "guid")

    def test_usable_in_create_payload(self):
        """Result can be merged into a create payload via **spread."""
        self.client._odata._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "contact",
                "ReferencingAttribute": "parentcustomerid",
                "ReferencedEntity": "account",
                "ReferencedAttribute": "accountid",
                "ReferencingEntityNavigationPropertyName": "parentcustomerid_account",
                "SchemaName": "contact_customer_accounts",
            },
        ]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        bind = self.client.query.odata_bind("contact", "account", "some-guid")
        payload = {"firstname": "Jane", "lastname": "Doe", **bind}
        self.assertIn("parentcustomerid_account@odata.bind", payload)
        self.assertEqual(payload["firstname"], "Jane")


if __name__ == "__main__":
    unittest.main()
