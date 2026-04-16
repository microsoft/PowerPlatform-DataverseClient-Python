# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for Entity and FieldDescriptor."""

from __future__ import annotations

import pytest

from PowerPlatform.Dataverse.models.entity import Entity, FieldDescriptor
from PowerPlatform.Dataverse.models.filters import FilterExpression
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.models.query_builder import QueryBuilder


# ---------------------------------------------------------------------------
# Minimal entity for testing
# ---------------------------------------------------------------------------


class Account(Entity, table="account", primary_key="accountid"):
    accountid  = FieldDescriptor("accountid",  str)
    name       = FieldDescriptor("name",       str)
    revenue    = FieldDescriptor("revenue",    float)
    statecode  = FieldDescriptor("statecode",  int)
    telephone1 = FieldDescriptor("telephone1", str)


# ---------------------------------------------------------------------------
# Annotation-based entity (no explicit FieldDescriptor assignments)
# ---------------------------------------------------------------------------


class Contact(Entity, table="contact", primary_key="contactid"):
    """Entity defined with plain annotations — FieldDescriptors are auto-created."""
    contactid:  str
    firstname:  str
    lastname:   str
    revenue:    float
    statecode:  int


class TestAnnotationBasedEntity:
    """FieldDescriptors are auto-created from type annotations."""

    def test_class_attribute_is_field_descriptor(self):
        """Annotated fields become FieldDescriptor instances on the class."""
        assert isinstance(Contact.firstname, FieldDescriptor)
        assert Contact.firstname.name == "firstname"

    def test_filter_dsl_works(self):
        """Annotation-based entity supports the same filter DSL as explicit descriptors."""
        expr = Contact.statecode == 0
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "statecode eq 0"

    def test_row_validates_fields(self):
        """row() validates fields derived from annotations."""
        d = Contact.row(firstname="Jane", lastname="Doe")
        assert d == {"firstname": "Jane", "lastname": "Doe"}

    def test_from_record_hydrates_typed_instance(self):
        """from_record() hydrates annotation-based entity correctly."""
        record = Record(id="id-1", table="contact", data={"firstname": "Jane", "statecode": 0})
        contact = Contact.from_record(record)
        assert isinstance(contact, Contact)
        assert contact.firstname == "Jane"
        assert contact.statecode == 0
        assert contact.lastname is None


# ---------------------------------------------------------------------------
# FieldDescriptor — class-level access produces FilterExpression
# ---------------------------------------------------------------------------


class TestFieldDescriptorClassAccess:
    """FieldDescriptor returns itself on class access and produces FilterExpression via operators."""

    def test_class_access_returns_descriptor(self):
        """Account.statecode on the class returns the FieldDescriptor itself."""
        assert isinstance(Account.statecode, FieldDescriptor)

    def test_eq_produces_filter_expression(self):
        """== on a descriptor produces a FilterExpression."""
        expr = Account.statecode == 0
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "statecode eq 0"

    def test_ne_produces_filter_expression(self):
        """!= on a descriptor produces a FilterExpression."""
        expr = Account.statecode != 1
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "statecode ne 1"

    def test_gt_produces_filter_expression(self):
        """> on a descriptor produces a FilterExpression."""
        expr = Account.revenue > 1_000_000
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "revenue gt 1000000"

    def test_ge_produces_filter_expression(self):
        """>= on a descriptor produces a FilterExpression."""
        expr = Account.revenue >= 500_000
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "revenue ge 500000"

    def test_lt_produces_filter_expression(self):
        """< on a descriptor produces a FilterExpression."""
        expr = Account.revenue < 100
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "revenue lt 100"

    def test_le_produces_filter_expression(self):
        """<= on a descriptor produces a FilterExpression."""
        expr = Account.revenue <= 999
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "revenue le 999"

    def test_and_composition(self):
        """& composes two descriptor expressions into an AND expression."""
        expr = (Account.statecode == 0) & (Account.revenue > 100_000)
        assert "and" in expr.to_odata()

    def test_or_composition(self):
        """| composes two descriptor expressions into an OR expression."""
        expr = (Account.statecode == 0) | (Account.statecode == 1)
        assert "or" in expr.to_odata()

    def test_not_composition(self):
        """~ negates a descriptor expression."""
        expr = ~(Account.statecode == 2)
        assert expr.to_odata() == "not (statecode eq 2)"

    def test_contains_method(self):
        """.contains() produces a contains() filter expression."""
        expr = Account.name.contains("Corp")
        assert expr.to_odata() == "contains(name, 'Corp')"

    def test_startswith_method(self):
        """.startswith() produces a startswith() filter expression."""
        expr = Account.name.startswith("Con")
        assert expr.to_odata() == "startswith(name, 'Con')"

    def test_endswith_method(self):
        """.endswith() produces an endswith() filter expression."""
        expr = Account.name.endswith("Ltd")
        assert expr.to_odata() == "endswith(name, 'Ltd')"

    def test_is_null_method(self):
        """.is_null() produces a null equality filter."""
        expr = Account.telephone1.is_null()
        assert expr.to_odata() == "telephone1 eq null"

    def test_is_not_null_method(self):
        """.is_not_null() produces a null inequality filter."""
        expr = Account.telephone1.is_not_null()
        assert expr.to_odata() == "telephone1 ne null"

    def test_in_method(self):
        """.in_() produces a Microsoft.Dynamics.CRM.In filter expression."""
        expr = Account.statecode.in_([0, 1])
        assert "Microsoft.Dynamics.CRM.In" in expr.to_odata()
        assert "statecode" in expr.to_odata()

    def test_between_method(self):
        """.between() produces a ge/le AND filter expression."""
        expr = Account.revenue.between(100_000, 500_000)
        odata = expr.to_odata()
        assert "revenue ge 100000" in odata
        assert "revenue le 500000" in odata

    def test_descriptor_is_hashable(self):
        """FieldDescriptor instances are hashable and can be used in sets."""
        s = {Account.name, Account.revenue, Account.statecode}
        assert len(s) == 3


# ---------------------------------------------------------------------------
# FieldDescriptor — instance-level access returns stored value
# ---------------------------------------------------------------------------


class TestFieldDescriptorInstanceAccess:
    """FieldDescriptor returns the stored value on instance access."""

    def test_instance_access_returns_value(self):
        """account.statecode returns the int value stored on the instance."""
        account = Account.__new__(Account)
        Account.statecode.__set__(account, 0)
        assert account.statecode == 0

    def test_unset_field_returns_none(self):
        """Accessing a field that was never set returns None."""
        account = Account.__new__(Account)
        assert account.revenue is None

    def test_different_instances_independent(self):
        """Setting a field on one instance does not affect another."""
        a1 = Account.__new__(Account)
        a2 = Account.__new__(Account)
        Account.name.__set__(a1, "Contoso")
        Account.name.__set__(a2, "Fabrikam")
        assert a1.name == "Contoso"
        assert a2.name == "Fabrikam"


# ---------------------------------------------------------------------------
# Entity.row()
# ---------------------------------------------------------------------------


class TestEntityRow:
    """Entity.row() builds a validated OData field dict."""

    def test_returns_dict_with_correct_keys(self):
        """row() maps attribute names to OData field name strings."""
        d = Account.row(name="Contoso", statecode=0)
        assert d == {"name": "Contoso", "statecode": 0}

    def test_unknown_field_raises_value_error(self):
        """row() raises ValueError for unrecognised field names."""
        with pytest.raises(ValueError, match="unknown field"):
            Account.row(nme="typo")

    def test_empty_row_returns_empty_dict(self):
        """row() with no arguments returns an empty dict."""
        assert Account.row() == {}

    def test_all_fields_accepted(self):
        """row() accepts every FieldDescriptor defined on the class."""
        d = Account.row(
            accountid="guid-1",
            name="Contoso",
            revenue=1_000_000.0,
            statecode=0,
            telephone1="555-0100",
        )
        assert len(d) == 5


# ---------------------------------------------------------------------------
# Entity.from_record()
# ---------------------------------------------------------------------------


class TestEntityFromRecord:
    """Entity.from_record() hydrates a typed entity instance from a Record."""

    def _make_record(self, data: dict, record_id: str = "guid-1") -> Record:
        return Record(id=record_id, table="account", data=data)

    def test_fields_populated_from_record_data(self):
        """from_record() sets each descriptor attribute from record.data."""
        record = self._make_record({"name": "Contoso", "statecode": 0, "revenue": 1_500_000.0})
        account = Account.from_record(record)
        assert account.name == "Contoso"
        assert account.statecode == 0
        assert account.revenue == 1_500_000.0

    def test_id_copied_from_record(self):
        """from_record() copies record.id to the entity instance."""
        record = self._make_record({}, record_id="abc-123")
        account = Account.from_record(record)
        assert account.id == "abc-123"

    def test_missing_field_is_none(self):
        """Fields absent from record.data are None on the hydrated instance."""
        record = self._make_record({"name": "Contoso"})
        account = Account.from_record(record)
        assert account.telephone1 is None

    def test_returns_correct_type(self):
        """from_record() returns an instance of the entity class."""
        record = self._make_record({})
        account = Account.from_record(record)
        assert isinstance(account, Account)


# ---------------------------------------------------------------------------
# QueryBuilder — typed path
# ---------------------------------------------------------------------------


class TestQueryBuilderTypedPath:
    """QueryBuilder accepts Entity classes and FieldDescriptors alongside strings."""

    def test_builder_accepts_entity_class(self):
        """QueryBuilder(Account) sets table to Account.__table__."""
        qb = QueryBuilder(Account)
        assert qb.table == "account"
        assert qb._entity_cls is Account

    def test_builder_accepts_string(self):
        """QueryBuilder('account') preserves existing string behaviour."""
        qb = QueryBuilder("account")
        assert qb.table == "account"
        assert qb._entity_cls is None

    def test_select_accepts_field_descriptors(self):
        """select() with FieldDescriptor arguments extracts field names."""
        qb = QueryBuilder(Account).select(Account.name, Account.revenue)
        assert qb._select == ["name", "revenue"]

    def test_select_accepts_strings(self):
        """select() with string arguments preserves existing behaviour."""
        qb = QueryBuilder("account").select("name", "revenue")
        assert qb._select == ["name", "revenue"]

    def test_select_mixed_strings_and_descriptors(self):
        """select() accepts a mix of strings and FieldDescriptors."""
        qb = QueryBuilder(Account).select(Account.name, "telephone1")
        assert qb._select == ["name", "telephone1"]

    def test_order_by_accepts_field_descriptor(self):
        """order_by() with a FieldDescriptor extracts the field name."""
        qb = QueryBuilder(Account).select(Account.name).order_by(Account.revenue, descending=True)
        assert qb._orderby == ["revenue desc"]

    def test_order_by_accepts_string(self):
        """order_by() with a string preserves existing behaviour."""
        qb = QueryBuilder("account").select("name").order_by("revenue", descending=True)
        assert qb._orderby == ["revenue desc"]

    def test_where_accepts_descriptor_expression(self):
        """where() accepts a FilterExpression produced by a FieldDescriptor."""
        qb = QueryBuilder(Account).select(Account.name).where(Account.statecode == 0)
        params = qb.build()
        assert params["filter"] == "statecode eq 0"

    def test_where_composes_multiple_descriptor_conditions(self):
        """Multiple where() calls using descriptors are AND-joined."""
        qb = (QueryBuilder(Account)
              .select(Account.name)
              .where(Account.statecode == 0)
              .where(Account.revenue > 100_000))
        params = qb.build()
        assert "statecode eq 0" in params["filter"]
        assert "revenue gt 100000" in params["filter"]
        assert " and " in params["filter"]

    def test_build_uses_correct_table(self):
        """build() emits the table name from the entity class."""
        params = QueryBuilder(Account).select(Account.name).build()
        assert params["table"] == "account"

    def test_invalid_entity_class_raises_type_error(self):
        """Passing an arbitrary class (not an Entity subclass) raises TypeError."""
        with pytest.raises(TypeError):
            QueryBuilder(object)  # type: ignore[arg-type]

    def test_execute_by_page_typed_yields_entity_pages(self):
        """execute(by_page=True) with a typed builder yields pages of entity instances."""
        from unittest.mock import MagicMock

        qb = QueryBuilder(Account)
        qb._query_ops = MagicMock()

        record1 = Record(id="id-1", table="account", data={"name": "Contoso", "revenue": 1.0})
        record2 = Record(id="id-2", table="account", data={"name": "Fabrikam", "revenue": 2.0})
        # Simulate two pages of one record each
        fake_pages = [[record1], [record2]]
        qb._query_ops._client.records.get.return_value = fake_pages
        qb.select(Account.name)

        pages = list(qb.execute(by_page=True))

        assert len(pages) == 2
        assert all(isinstance(item, Account) for page in pages for item in page)
        assert pages[0][0].name == "Contoso"
        assert pages[1][0].name == "Fabrikam"

    def test_string_and_typed_produce_same_odata_params(self):
        """String-based and typed builders produce identical build() output."""
        from PowerPlatform.Dataverse.models.filters import eq, gt

        string_params = (QueryBuilder("account")
                         .select("name", "revenue")
                         .where(eq("statecode", 0))
                         .where(gt("revenue", 100_000))
                         .order_by("revenue", descending=True)
                         .top(10)
                         .build())

        typed_params = (QueryBuilder(Account)
                        .select(Account.name, Account.revenue)
                        .where(Account.statecode == 0)
                        .where(Account.revenue > 100_000)
                        .order_by(Account.revenue, descending=True)
                        .top(10)
                        .build())

        assert string_params["filter"] == typed_params["filter"]
        assert string_params["select"] == typed_params["select"]
        assert string_params["orderby"] == typed_params["orderby"]
        assert string_params["top"] == typed_params["top"]
        assert string_params["table"] == typed_params["table"]
