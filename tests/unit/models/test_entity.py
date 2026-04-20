# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for the typed entity model: Entity, field descriptors, picklist, boolean, lookup."""

import unittest
from decimal import Decimal
from datetime import datetime

from PowerPlatform.Dataverse.models.entity import Entity, _EntityT
from PowerPlatform.Dataverse.models.datatypes import (
    Text, Memo, Integer, BigInt, DecimalNumber, Double, Money, DateTime, Guid, _FieldBase,
)
from PowerPlatform.Dataverse.models.lookup import Lookup, CustomerLookup
from PowerPlatform.Dataverse.models.picklist import PicklistBase, PicklistOption, MultiPicklist
from PowerPlatform.Dataverse.models.boolean import BooleanBase, BooleanOption
from PowerPlatform.Dataverse.models.filters import _ComparisonFilter


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

class IndustryCode(PicklistBase):
    Technology = PicklistOption(7, "Technology")
    Consulting = PicklistOption(8, "Consulting")
    Finance = PicklistOption(6, "Finance")


class CreditOnHold(BooleanBase):
    Yes = BooleanOption(True, "Yes")
    No = BooleanOption(False, "No")


class Account(Entity, table="account", primary_key="accountid", entity_set="accounts"):
    accountid = Guid(writable_on_create=False, writable_on_update=False)
    name = Text(nullable=False, max_length=160)
    employees = Integer(min_value=0)
    revenue = Money()
    createdon = DateTime()
    industrycode = IndustryCode()
    creditonhold = CreditOnHold()
    primarycontactid = Lookup(target="contact")
    tags = MultiPicklist()


class Contact(Entity, table="contact", primary_key="contactid"):
    contactid = Guid(writable_on_create=False, writable_on_update=False)
    fullname = Text()
    accountid = Lookup(target="account")


# ---------------------------------------------------------------------------
# Entity metadata
# ---------------------------------------------------------------------------

class TestEntityMetadata(unittest.TestCase):
    def test_logical_name(self):
        self.assertEqual(Account._logical_name, "account")

    def test_primary_id(self):
        self.assertEqual(Account._primary_id, "accountid")

    def test_entity_set(self):
        self.assertEqual(Account._entity_set, "accounts")

    def test_defaults_empty_string(self):
        self.assertEqual(Contact._entity_set, "")
        self.assertEqual(Contact._label, "")

    def test_metadata_not_inherited_by_sibling(self):
        self.assertEqual(Contact._logical_name, "contact")
        self.assertNotEqual(Contact._logical_name, Account._logical_name)


# ---------------------------------------------------------------------------
# Entity construction
# ---------------------------------------------------------------------------

class TestEntityConstruction(unittest.TestCase):
    def test_empty_construction(self):
        a = Account()
        self.assertEqual(a._data, {})

    def test_kwarg_construction(self):
        a = Account(name="Contoso", employees=500)
        self.assertEqual(a._data, {"name": "Contoso", "employees": 500})

    def test_kwarg_construction_accesses_via_descriptor(self):
        a = Account(name="Contoso")
        self.assertEqual(a.name, "Contoso")
        self.assertIsNone(a.employees)

    def test_setattr_routes_through_descriptor(self):
        a = Account()
        a.name = "Fabrikam"
        self.assertEqual(a._data["name"], "Fabrikam")

    def test_setattr_non_descriptor_sets_on_object(self):
        a = Account()
        a._custom = "x"
        self.assertEqual(a._custom, "x")
        self.assertNotIn("_custom", a._data)

    def test_repr(self):
        a = Account(name="X")
        r = repr(a)
        self.assertIn("Account", r)
        self.assertIn("account", r)

    def test_eq_same_type_same_data(self):
        a1 = Account(name="X")
        a2 = Account(name="X")
        self.assertEqual(a1, a2)

    def test_eq_same_type_different_data(self):
        a1 = Account(name="X")
        a2 = Account(name="Y")
        self.assertNotEqual(a1, a2)

    def test_eq_different_type(self):
        a = Account(name="X")
        c = Contact(fullname="X")
        self.assertNotEqual(a, c)

    def test_hash_is_id(self):
        a = Account(name="X")
        self.assertEqual(hash(a), id(a))


# ---------------------------------------------------------------------------
# Field descriptor protocol
# ---------------------------------------------------------------------------

class TestFieldDescriptorProtocol(unittest.TestCase):
    def test_class_access_returns_descriptor(self):
        self.assertIsInstance(Account.name, Text)

    def test_set_name_sets_logical_name(self):
        self.assertEqual(Account.name._logical_name, "name")
        self.assertEqual(Account.employees._logical_name, "employees")

    def test_instance_access_returns_value(self):
        a = Account(name="Contoso")
        self.assertEqual(a.name, "Contoso")

    def test_unset_field_returns_none(self):
        a = Account()
        self.assertIsNone(a.name)

    def test_overwrite_field(self):
        a = Account(name="A")
        a.name = "B"
        self.assertEqual(a.name, "B")

    def test_all_field_types_round_trip(self):
        now = datetime(2024, 1, 1, 12, 0, 0)
        a = Account(
            name="Acme",
            employees=100,
            revenue=Decimal("9999.99"),
            createdon=now,
            industrycode=7,
            creditonhold=True,
        )
        self.assertEqual(a.name, "Acme")
        self.assertEqual(a.employees, 100)
        self.assertEqual(a.revenue, Decimal("9999.99"))
        self.assertEqual(a.createdon, now)
        self.assertEqual(a.industrycode, 7)
        self.assertEqual(a.creditonhold, True)


# ---------------------------------------------------------------------------
# Filter operators on descriptors
# ---------------------------------------------------------------------------

class TestFieldFilterOperators(unittest.TestCase):
    def _check_filter(self, expr, logical_name, op, value):
        self.assertIsInstance(expr, _ComparisonFilter)
        self.assertEqual(expr.column, logical_name)
        self.assertEqual(expr.op, op)
        self.assertEqual(expr.value, value)

    def test_eq(self):
        self._check_filter(Account.name == "X", "name", "eq", "X")

    def test_ne(self):
        self._check_filter(Account.name != "X", "name", "ne", "X")

    def test_gt(self):
        self._check_filter(Account.employees > 100, "employees", "gt", 100)

    def test_ge(self):
        self._check_filter(Account.employees >= 100, "employees", "ge", 100)

    def test_lt(self):
        self._check_filter(Account.employees < 100, "employees", "lt", 100)

    def test_le(self):
        self._check_filter(Account.employees <= 100, "employees", "le", 100)

    def test_hash_is_id(self):
        d = Account.name
        self.assertEqual(hash(d), id(d))


# ---------------------------------------------------------------------------
# Text descriptor metadata
# ---------------------------------------------------------------------------

class TestTextDescriptor(unittest.TestCase):
    def test_nullable_false(self):
        self.assertFalse(Account.name.nullable)

    def test_max_length(self):
        self.assertEqual(Account.name.max_length, 160)

    def test_writable_flags_default_true(self):
        self.assertTrue(Account.name.writable_on_create)
        self.assertTrue(Account.name.writable_on_update)


# ---------------------------------------------------------------------------
# Guid helpers
# ---------------------------------------------------------------------------

class TestGuid(unittest.TestCase):
    def test_new_returns_valid_guid_string(self):
        import re
        g = Guid.new()
        self.assertRegex(g, r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

    def test_empty_returns_zero_guid(self):
        self.assertEqual(Guid.empty(), "00000000-0000-0000-0000-000000000000")

    def test_writable_on_create_false(self):
        self.assertFalse(Account.accountid.writable_on_create)
        self.assertFalse(Account.accountid.writable_on_update)


# ---------------------------------------------------------------------------
# Picklist
# ---------------------------------------------------------------------------

class TestPicklistOption(unittest.TestCase):
    def test_int_value(self):
        self.assertEqual(int(IndustryCode.Technology), 7)

    def test_label(self):
        self.assertEqual(IndustryCode.Technology.label, "Technology")

    def test_str_returns_label(self):
        self.assertEqual(str(IndustryCode.Technology), "Technology")

    def test_repr(self):
        self.assertIn("PicklistOption", repr(IndustryCode.Technology))


class TestPicklistBase(unittest.TestCase):
    def test_options_returns_all(self):
        opts = IndustryCode.options()
        self.assertEqual(set(opts.keys()), {"Technology", "Consulting", "Finance"})

    def test_from_value_found(self):
        opt = IndustryCode.from_value(7)
        self.assertIsNotNone(opt)
        self.assertEqual(opt.label, "Technology")

    def test_from_value_not_found(self):
        self.assertIsNone(IndustryCode.from_value(999))

    def test_from_label_found(self):
        opt = IndustryCode.from_label("consulting")
        self.assertIsNotNone(opt)
        self.assertEqual(int(opt), 8)

    def test_from_label_not_found(self):
        self.assertIsNone(IndustryCode.from_label("Unknown"))

    def test_filter_operator(self):
        expr = Account.industrycode == IndustryCode.Technology
        self.assertIsInstance(expr, _ComparisonFilter)
        self.assertEqual(expr.column, "industrycode")

    def test_instance_access_returns_int(self):
        a = Account(industrycode=7)
        self.assertEqual(a.industrycode, 7)


# ---------------------------------------------------------------------------
# Boolean
# ---------------------------------------------------------------------------

class TestBooleanOption(unittest.TestCase):
    def test_true_option_bool(self):
        self.assertTrue(bool(CreditOnHold.Yes))

    def test_false_option_bool(self):
        self.assertFalse(bool(CreditOnHold.No))

    def test_label(self):
        self.assertEqual(CreditOnHold.Yes.label, "Yes")

    def test_str_returns_label(self):
        self.assertEqual(str(CreditOnHold.Yes), "Yes")


class TestBooleanBase(unittest.TestCase):
    def test_true_option(self):
        opt = CreditOnHold.true_option()
        self.assertIsNotNone(opt)
        self.assertTrue(bool(opt))

    def test_false_option(self):
        opt = CreditOnHold.false_option()
        self.assertIsNotNone(opt)
        self.assertFalse(bool(opt))

    def test_from_value_true(self):
        opt = CreditOnHold.from_value(True)
        self.assertTrue(bool(opt))

    def test_from_value_false(self):
        opt = CreditOnHold.from_value(False)
        self.assertFalse(bool(opt))

    def test_from_label(self):
        opt = CreditOnHold.from_label("yes")
        self.assertIsNotNone(opt)
        self.assertTrue(bool(opt))

    def test_from_label_not_found(self):
        self.assertIsNone(CreditOnHold.from_label("Maybe"))

    def test_filter_operator(self):
        expr = Account.creditonhold == True
        self.assertIsInstance(expr, _ComparisonFilter)
        self.assertEqual(expr.column, "creditonhold")

    def test_invalid_subclass_no_options_allowed(self):
        class EmptyBoolean(BooleanBase):
            pass
        # No options defined — validation is only triggered when options ARE present
        # but unbalanced; an empty subclass is allowed (generator intermediate).

    def test_invalid_subclass_two_true_options_raises(self):
        with self.assertRaises(TypeError):
            class BadBoolean(BooleanBase):
                A = BooleanOption(True, "A")
                B = BooleanOption(True, "B")

    def test_invalid_subclass_two_false_options_raises(self):
        with self.assertRaises(TypeError):
            class BadBoolean2(BooleanBase):
                A = BooleanOption(True, "A")
                B = BooleanOption(False, "B1")
                C = BooleanOption(False, "B2")


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------

class TestLookup(unittest.TestCase):
    def test_target(self):
        self.assertEqual(Account.primarycontactid.target, "contact")

    def test_logical_name_set_by_set_name(self):
        self.assertEqual(Account.primarycontactid._logical_name, "primarycontactid")

    def test_instance_access(self):
        a = Account(primarycontactid="some-guid")
        self.assertEqual(a.primarycontactid, "some-guid")

    def test_filter_operator(self):
        expr = Account.primarycontactid == "some-guid"
        self.assertIsInstance(expr, _ComparisonFilter)
        self.assertEqual(expr.column, "primarycontactid")

    def test_repr(self):
        self.assertIn("Lookup", repr(Account.primarycontactid))


class TestCustomerLookup(unittest.TestCase):
    def test_targets_tuple(self):
        cl = CustomerLookup(targets=("account", "contact"))
        self.assertEqual(cl.targets, ("account", "contact"))

    def test_empty_targets_default(self):
        cl = CustomerLookup()
        self.assertEqual(cl.targets, ())

    def test_repr(self):
        cl = CustomerLookup(targets=("account",))
        self.assertIn("CustomerLookup", repr(cl))


# ---------------------------------------------------------------------------
# Entity.fields()
# ---------------------------------------------------------------------------

class TestEntityFields(unittest.TestCase):
    def test_fields_returns_all_descriptors(self):
        f = Account.fields()
        self.assertIn("name", f)
        self.assertIn("employees", f)
        self.assertIn("accountid", f)
        self.assertIn("industrycode", f)
        self.assertIn("creditonhold", f)
        self.assertIn("primarycontactid", f)

    def test_fields_excludes_private(self):
        for name in Account.fields():
            self.assertFalse(name.startswith("_"), f"_private field leaked: {name}")

    def test_fields_values_are_descriptors(self):
        for name, desc in Account.fields().items():
            self.assertTrue(getattr(desc, "_is_field_descriptor", False))

    def test_fields_inheritance(self):
        class SpecialAccount(Account):
            notes = Memo()

        f = SpecialAccount.fields()
        self.assertIn("name", f)
        self.assertIn("notes", f)


# ---------------------------------------------------------------------------
# Entity.as_dict()
# ---------------------------------------------------------------------------

class TestEntityAsDict(unittest.TestCase):
    def test_returns_copy(self):
        a = Account(name="Contoso")
        d = a.as_dict()
        d["name"] = "Modified"
        self.assertEqual(a.name, "Contoso")

    def test_only_set_fields(self):
        a = Account(name="Contoso", employees=10)
        d = a.as_dict()
        self.assertEqual(set(d.keys()), {"name", "employees"})


# ---------------------------------------------------------------------------
# Entity.from_dict()
# ---------------------------------------------------------------------------

class TestEntityFromDict(unittest.TestCase):
    def test_hydrates_fields(self):
        a = Account.from_dict({"name": "Contoso", "employees": 100})
        self.assertEqual(a.name, "Contoso")
        self.assertEqual(a.employees, 100)

    def test_empty_dict(self):
        a = Account.from_dict({})
        self.assertIsNone(a.name)

    def test_extra_keys_stored_but_not_accessible_via_descriptor(self):
        a = Account.from_dict({"name": "X", "@odata.etag": "W/\"123\""})
        self.assertEqual(a.name, "X")
        # Extra key is in _data but not on any descriptor
        self.assertIn("@odata.etag", a._data)

    def test_returns_correct_type(self):
        a = Account.from_dict({"name": "X"})
        self.assertIsInstance(a, Account)


# ---------------------------------------------------------------------------
# Entity.to_create_payload() / to_update_payload()
# ---------------------------------------------------------------------------

class TestEntityPayloads(unittest.TestCase):
    def _build_full(self):
        return Account(
            accountid="guid-1",
            name="Contoso",
            employees=500,
        )

    def test_to_create_payload_excludes_writable_on_create_false(self):
        a = self._build_full()
        payload = a.to_create_payload()
        self.assertNotIn("accountid", payload.as_dict())
        self.assertIn("name", payload.as_dict())
        self.assertIn("employees", payload.as_dict())

    def test_to_update_payload_excludes_writable_on_update_false(self):
        a = self._build_full()
        payload = a.to_update_payload()
        self.assertNotIn("accountid", payload.as_dict())
        self.assertIn("name", payload.as_dict())

    def test_payload_returns_same_type(self):
        a = Account(name="X")
        self.assertIsInstance(a.to_create_payload(), Account)
        self.assertIsInstance(a.to_update_payload(), Account)

    def test_payload_does_not_mutate_original(self):
        a = Account(accountid="g", name="X")
        a.to_create_payload()
        self.assertIn("accountid", a.as_dict())

    def test_unset_fields_not_in_payload(self):
        a = Account(name="X")
        payload = a.to_create_payload()
        self.assertNotIn("employees", payload.as_dict())


# ---------------------------------------------------------------------------
# QueryBuilder with Entity type
# ---------------------------------------------------------------------------

class TestQueryBuilderWithEntity(unittest.TestCase):
    def test_builder_accepts_entity_class(self):
        from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
        qb = QueryBuilder(Account)
        self.assertEqual(qb.table, "account")

    def test_select_with_descriptor(self):
        from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
        qb = QueryBuilder(Account).select(Account.name, Account.employees)
        built = qb.build()
        self.assertIn("name", built["select"])
        self.assertIn("employees", built["select"])

    def test_select_mixed_str_and_descriptor(self):
        from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
        qb = QueryBuilder(Account).select("revenue", Account.name)
        built = qb.build()
        self.assertIn("revenue", built["select"])
        self.assertIn("name", built["select"])

    def test_order_by_with_descriptor(self):
        from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
        qb = QueryBuilder(Account).order_by(Account.name)
        built = qb.build()
        self.assertIn("name", built["orderby"][0])

    def test_where_with_descriptor_expression(self):
        from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
        qb = QueryBuilder(Account).where(Account.name == "Contoso")
        built = qb.build()
        self.assertIn("name", built["filter"])
        self.assertIn("Contoso", built["filter"])


if __name__ == "__main__":
    unittest.main()
