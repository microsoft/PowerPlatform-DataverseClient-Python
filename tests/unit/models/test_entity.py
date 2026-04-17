# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for Entity and Field."""

from __future__ import annotations

import pytest

from PowerPlatform.Dataverse.models.entity import Entity, Field, NavField, resolve_table, resolve_table_pair
from PowerPlatform.Dataverse.models.filters import FilterExpression
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.models.query_builder import ExpandOption, QueryBuilder


# ---------------------------------------------------------------------------
# Minimal entity for testing
# ---------------------------------------------------------------------------


class Account(Entity, table="account", primary_key="accountid"):
    accountid  = Field("accountid",  str)
    name       = Field("name",       str)
    revenue    = Field("revenue",    float)
    statecode  = Field("statecode",  int)
    telephone1 = Field("telephone1", str)


# ---------------------------------------------------------------------------
# Annotation-based entity (no explicit Field assignments)
# ---------------------------------------------------------------------------


class Contact(Entity, table="contact", primary_key="contactid"):
    """Entity defined with plain annotations — Fields are auto-created."""
    contactid:  str
    firstname:  str
    lastname:   str
    revenue:    float
    statecode:  int


class TestAnnotationBasedEntity:
    """Fields are auto-created from type annotations."""

    def test_class_attribute_is_field_descriptor(self):
        """Annotated fields become Field instances on the class."""
        assert isinstance(Contact.firstname, Field)
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
# Field — class-level access produces FilterExpression
# ---------------------------------------------------------------------------


class TestFieldClassAccess:
    """Field returns itself on class access and produces FilterExpression via operators."""

    def test_class_access_returns_descriptor(self):
        """Account.statecode on the class returns the Field itself."""
        assert isinstance(Account.statecode, Field)

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
        """Field instances are hashable and can be used in sets."""
        s = {Account.name, Account.revenue, Account.statecode}
        assert len(s) == 3


# ---------------------------------------------------------------------------
# Field — instance-level access returns stored value
# ---------------------------------------------------------------------------


class TestFieldInstanceAccess:
    """Field returns the stored value on instance access."""

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
        """row() accepts every Field defined on the class."""
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
    """QueryBuilder accepts Entity classes and Fields alongside strings."""

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
        """select() with Field arguments extracts field names."""
        qb = QueryBuilder(Account).select(Account.name, Account.revenue)
        assert qb._select == ["name", "revenue"]

    def test_select_accepts_strings(self):
        """select() with string arguments preserves existing behaviour."""
        qb = QueryBuilder("account").select("name", "revenue")
        assert qb._select == ["name", "revenue"]

    def test_select_mixed_strings_and_descriptors(self):
        """select() accepts a mix of strings and Fields."""
        qb = QueryBuilder(Account).select(Account.name, "telephone1")
        assert qb._select == ["name", "telephone1"]

    def test_order_by_accepts_field_descriptor(self):
        """order_by() with a Field extracts the field name."""
        qb = QueryBuilder(Account).select(Account.name).order_by(Account.revenue, descending=True)
        assert qb._orderby == ["revenue desc"]

    def test_order_by_accepts_string(self):
        """order_by() with a string preserves existing behaviour."""
        qb = QueryBuilder("account").select("name").order_by("revenue", descending=True)
        assert qb._orderby == ["revenue desc"]

    def test_where_accepts_descriptor_expression(self):
        """where() accepts a FilterExpression produced by a Field."""
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


# ---------------------------------------------------------------------------
# Field — schema_name and dataverse_type
# ---------------------------------------------------------------------------


class TypedProduct(Entity, table="new_Product", primary_key="new_productid"):
    """Entity with full metadata for table creation."""
    new_productid = Field("new_productid", str)
    new_title     = Field("new_title",     str,   schema_name="new_Title",  dataverse_type="string")
    new_price     = Field("new_price",     float, schema_name="new_Price",  dataverse_type="decimal")
    new_quantity  = Field("new_quantity",  int,   schema_name="new_Quantity", dataverse_type="int")


class TestFieldMetadata:
    """Field carries schema_name and dataverse_type for table creation."""

    def test_schema_name_defaults_to_name(self):
        """When schema_name is omitted, it defaults to the logical name."""
        d = Field("statecode", int)
        assert d.schema_name == "statecode"

    def test_schema_name_explicit(self):
        """Explicit schema_name is stored correctly."""
        d = Field("new_title", str, schema_name="new_Title")
        assert d.schema_name == "new_Title"

    def test_dataverse_type_none_by_default(self):
        """dataverse_type is None when not provided."""
        d = Field("statecode", int)
        assert d.dataverse_type is None

    def test_dataverse_type_string(self):
        """dataverse_type stores a string type."""
        d = Field("new_title", str, dataverse_type="string")
        assert d.dataverse_type == "string"

    def test_dataverse_type_enum(self):
        """dataverse_type stores an IntEnum subclass."""
        from enum import IntEnum

        class Priority(IntEnum):
            LOW = 1
            HIGH = 3

        d = Field("new_priority", int, dataverse_type=Priority)
        assert d.dataverse_type is Priority

    def test_filter_dsl_still_works_with_metadata(self):
        """Adding schema_name/dataverse_type doesn't break the filter DSL."""
        expr = TypedProduct.new_price > 100.0
        assert isinstance(expr, FilterExpression)
        assert expr.to_odata() == "new_price gt 100.0"


# ---------------------------------------------------------------------------
# Entity.columns_schema()
# ---------------------------------------------------------------------------


class TestEntityColumnsSchema:
    """Entity.columns_schema() derives a column dict from Fields."""

    def test_returns_schema_name_keys(self):
        """columns_schema() uses the schema_name (not the logical name) as keys."""
        schema = TypedProduct.columns_schema()
        assert "new_Title" in schema
        assert "new_Price" in schema
        assert "new_Quantity" in schema

    def test_returns_dataverse_type_values(self):
        """columns_schema() maps schema names to their dataverse_type."""
        schema = TypedProduct.columns_schema()
        assert schema["new_Title"] == "string"
        assert schema["new_Price"] == "decimal"
        assert schema["new_Quantity"] == "int"

    def test_omits_fields_without_dataverse_type(self):
        """Fields with no dataverse_type (e.g. primary key) are excluded."""
        schema = TypedProduct.columns_schema()
        # new_productid has no dataverse_type
        assert "new_productid" not in schema
        assert "new_Product" not in schema  # schema_name of pk is also absent

    def test_empty_when_no_typed_descriptors(self):
        """Returns empty dict when no descriptors carry dataverse_type."""

        class Bare(Entity, table="new_Bare", primary_key="new_bareid"):
            new_bareid = Field("new_bareid", str)

        assert Bare.columns_schema() == {}

    def test_annotation_only_fields_excluded(self):
        """Annotation-only descriptors (auto-created) have no dataverse_type — excluded."""

        class AnnotOnly(Entity, table="new_AnnotOnly", primary_key="new_annotid"):
            new_annotid: str
            new_name: str

        assert AnnotOnly.columns_schema() == {}


# ---------------------------------------------------------------------------
# resolve_table_pair()
# ---------------------------------------------------------------------------


class TestResolveTablePair:
    """resolve_table_pair() returns (table_str, entity_cls)."""

    def test_string_returns_string_and_none(self):
        table_str, entity_cls = resolve_table_pair("account")
        assert table_str == "account"
        assert entity_cls is None

    def test_entity_class_returns_table_and_class(self):
        table_str, entity_cls = resolve_table_pair(Account)
        assert table_str == "account"
        assert entity_cls is Account

    def test_invalid_type_raises_type_error(self):
        with pytest.raises(TypeError):
            resolve_table_pair(123)  # type: ignore[arg-type]

    def test_entity_without_table_raises_value_error(self):
        class NoTable(Entity):
            pass

        with pytest.raises(ValueError, match="__table__"):
            resolve_table_pair(NoTable)

    def test_resolve_table_still_works(self):
        """resolve_table() still returns just the string (backward compat)."""
        assert resolve_table(Account) == "account"
        assert resolve_table("contact") == "contact"


# ---------------------------------------------------------------------------
# NavField — navigation property descriptor
# ---------------------------------------------------------------------------


class Contact2(Entity, table="contact", primary_key="contactid"):
    contactid = Field("contactid", str)
    firstname = Field("firstname", str)


class AccountWithNav(Entity, table="account", primary_key="accountid"):
    accountid        = Field("accountid", str)
    name             = Field("name", str)
    primarycontactid = NavField("primarycontactid", Contact2)
    Account_Tasks    = NavField("Account_Tasks")


class TestNavFieldClassAccess:
    """NavField returns itself on class-level access."""

    def test_class_access_returns_descriptor(self):
        """AccountWithNav.primarycontactid on the class returns the NavField itself."""
        assert isinstance(AccountWithNav.primarycontactid, NavField)

    def test_name_attribute(self):
        """NavField stores the OData navigation property name."""
        assert AccountWithNav.primarycontactid.name == "primarycontactid"
        assert AccountWithNav.Account_Tasks.name == "Account_Tasks"

    def test_related_entity_stored(self):
        """related_entity is stored when provided."""
        assert AccountWithNav.primarycontactid.related_entity is Contact2

    def test_related_entity_none_when_omitted(self):
        """related_entity is None when not provided."""
        assert AccountWithNav.Account_Tasks.related_entity is None

    def test_repr_with_related_entity(self):
        """__repr__ includes related entity name."""
        assert "primarycontactid" in repr(AccountWithNav.primarycontactid)
        assert "Contact2" in repr(AccountWithNav.primarycontactid)

    def test_repr_without_related_entity(self):
        """__repr__ shows None for missing related entity."""
        assert "None" in repr(AccountWithNav.Account_Tasks)


class TestNavFieldInstanceAccess:
    """NavField stores and retrieves per-instance data."""

    def test_unset_returns_none(self):
        """Accessing a NavField that was never set returns None."""
        account = AccountWithNav.__new__(AccountWithNav)
        assert account.primarycontactid is None

    def test_set_and_get_dict(self):
        """Setting a NavField to a dict and reading it back works."""
        account = AccountWithNav.__new__(AccountWithNav)
        data = {"contactid": "c-1", "firstname": "Jane"}
        AccountWithNav.primarycontactid.__set__(account, data)
        assert account.primarycontactid == data

    def test_set_and_get_list(self):
        """Setting a NavField to a list (collection) and reading it back works."""
        account = AccountWithNav.__new__(AccountWithNav)
        tasks = [{"subject": "Call"}, {"subject": "Email"}]
        AccountWithNav.Account_Tasks.__set__(account, tasks)
        assert account.Account_Tasks == tasks

    def test_different_instances_independent(self):
        """Setting a NavField on one instance does not affect another."""
        a1 = AccountWithNav.__new__(AccountWithNav)
        a2 = AccountWithNav.__new__(AccountWithNav)
        AccountWithNav.primarycontactid.__set__(a1, {"contactid": "c-1"})
        assert a1.primarycontactid == {"contactid": "c-1"}
        assert a2.primarycontactid is None


class TestNavFieldFromRecord:
    """NavField values are populated via Entity.from_record()."""

    def test_nav_field_populated_from_record_data(self):
        """from_record() populates NavField from expanded data in record.data."""
        contact_data = {"contactid": "c-1", "firstname": "Jane"}
        record = Record(
            id="a-1",
            table="account",
            data={"name": "Contoso", "primarycontactid": contact_data},
        )
        account = AccountWithNav.from_record(record)
        assert account.primarycontactid == contact_data

    def test_nav_field_none_when_not_expanded(self):
        """NavField is None when no expanded data is present in the record."""
        record = Record(id="a-1", table="account", data={"name": "Contoso"})
        account = AccountWithNav.from_record(record)
        assert account.primarycontactid is None

    def test_nav_field_list_value(self):
        """from_record() handles collection NavField (list) correctly."""
        tasks = [{"subject": "Call"}, {"subject": "Email"}]
        record = Record(
            id="a-1",
            table="account",
            data={"name": "Contoso", "Account_Tasks": tasks},
        )
        account = AccountWithNav.from_record(record)
        assert account.Account_Tasks == tasks


class TestNavFieldFluentMethods:
    """NavField fluent methods produce correct ExpandOption objects."""

    def test_select_with_strings_returns_expand_option(self):
        """select() with string column names returns an ExpandOption."""
        opt = AccountWithNav.Account_Tasks.select("subject", "createdon")
        assert isinstance(opt, ExpandOption)

    def test_select_odata_contains_relation_name(self):
        """ExpandOption from select() uses the NavField name as the relation."""
        opt = AccountWithNav.Account_Tasks.select("subject")
        odata = opt.to_odata()
        assert "Account_Tasks" in odata

    def test_select_odata_contains_columns(self):
        """ExpandOption from select() includes the requested columns."""
        opt = AccountWithNav.Account_Tasks.select("subject", "createdon")
        odata = opt.to_odata()
        assert "subject" in odata
        assert "createdon" in odata

    def test_select_with_field_descriptors(self):
        """select() accepts Field descriptors and extracts their names."""
        opt = AccountWithNav.primarycontactid.select(Contact2.firstname)
        odata = opt.to_odata()
        assert "firstname" in odata

    def test_filter_where_returns_expand_option(self):
        """filter_where() returns an ExpandOption with a nested $filter."""
        expr = Contact2.firstname == "Jane"
        opt = AccountWithNav.primarycontactid.filter_where(expr)
        assert isinstance(opt, ExpandOption)
        odata = opt.to_odata()
        assert "firstname eq 'Jane'" in odata

    def test_order_by_with_string(self):
        """order_by() with a string column returns ExpandOption with $orderby."""
        opt = AccountWithNav.Account_Tasks.order_by("createdon", descending=True)
        assert isinstance(opt, ExpandOption)
        odata = opt.to_odata()
        assert "createdon desc" in odata

    def test_order_by_with_field_descriptor(self):
        """order_by() with a Field descriptor extracts the field name."""
        opt = AccountWithNav.primarycontactid.order_by(Contact2.firstname)
        odata = opt.to_odata()
        assert "firstname" in odata

    def test_top_returns_expand_option(self):
        """top() returns an ExpandOption with $top."""
        opt = AccountWithNav.Account_Tasks.top(3)
        assert isinstance(opt, ExpandOption)
        odata = opt.to_odata()
        assert "$top=3" in odata

    def test_chained_fluent_methods(self):
        """Chained select().order_by().top() produces valid ExpandOption odata."""
        opt = (AccountWithNav.Account_Tasks
               .select("subject", "createdon")
               .order_by("createdon", descending=True)
               .top(5))
        odata = opt.to_odata()
        assert "Account_Tasks" in odata
        assert "subject" in odata
        assert "createdon desc" in odata
        assert "$top=5" in odata


class TestNavFieldInQueryBuilder:
    """QueryBuilder.expand() accepts NavField and NavField-derived ExpandOption."""

    def test_expand_with_nav_field_adds_relation_name(self):
        """expand(NavField) adds the navigation property name as a plain expand."""
        qb = QueryBuilder(AccountWithNav).expand(AccountWithNav.primarycontactid)
        params = qb.build()
        assert "primarycontactid" in params["expand"]

    def test_expand_with_nav_field_collection(self):
        """expand() works with a collection NavField too."""
        qb = QueryBuilder(AccountWithNav).expand(AccountWithNav.Account_Tasks)
        params = qb.build()
        assert "Account_Tasks" in params["expand"]

    def test_expand_with_nav_field_select_option(self):
        """expand() with a NavField.select() result produces nested $select."""
        opt = AccountWithNav.Account_Tasks.select("subject", "createdon")
        qb = QueryBuilder(AccountWithNav).expand(opt)
        params = qb.build()
        expand_str = " ".join(params["expand"])
        assert "subject" in expand_str
        assert "Account_Tasks" in expand_str

    def test_expand_accepts_mixed_types(self):
        """expand() can mix plain strings, NavFields, and ExpandOptions."""
        qb = (QueryBuilder(AccountWithNav)
              .expand("revenue")
              .expand(AccountWithNav.primarycontactid)
              .expand(AccountWithNav.Account_Tasks.top(3)))
        params = qb.build()
        expand_str = " ".join(params["expand"])
        assert "revenue" in expand_str
        assert "primarycontactid" in expand_str
        assert "Account_Tasks" in expand_str
