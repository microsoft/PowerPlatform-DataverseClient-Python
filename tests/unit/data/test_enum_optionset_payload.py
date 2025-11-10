# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from enum import Enum, IntEnum

from dataverse_sdk.data.odata import ODataClient

class DummyAuth:
    def acquire_token(self, scope):  # pragma: no cover - simple stub
        class T:
            access_token = "token"
        return T()

class DummyConfig:
    """Minimal config stub providing attributes ODataClient.__init__ expects."""

    def __init__(self, language_code=1033):
        self.language_code = language_code
        # HTTP settings referenced during ODataClient construction
        self.http_retries = 0
        self.http_backoff = 0
        self.http_timeout = 5

def _make_client(lang=1033):
    return ODataClient(DummyAuth(), "https://org.example", DummyConfig(language_code=lang))

def _labels_for(option):
    label = option.get("Label") or {}
    locs = label.get("LocalizedLabels") or []
    return {l.get("LanguageCode"): l.get("Label") for l in locs if isinstance(l, dict)}

def test_enum_basic_no_labels_uses_member_names():
    class Basic(IntEnum):
        One = 10
        Two = 20

    c = _make_client(lang=1033)
    payload = c._enum_optionset_payload("new_Status", Basic)
    assert payload["@odata.type"].endswith("PicklistAttributeMetadata")
    opts = payload["OptionSet"]["Options"]
    assert [o["Value"] for o in opts] == [10, 20]
    # Each option should have a 1033 label with the member name
    for o in opts:
        labels = _labels_for(o)
        assert 1033 in labels
        assert labels[1033] in ("One", "Two")

def test_enum_with_multilanguage_labels_includes_all():
    class ML(IntEnum):
        Active = 1
        Inactive = 2
        Archived = 5
        __labels__ = {
            1033: {"Active": "Active", "Inactive": "Inactive", "Archived": "Archived"},
            1036: {"Active": "Actif", "Inactive": "Inactif", "Archived": "Archivé"},
        }

    c = _make_client(lang=1033)
    payload = c._enum_optionset_payload("new_Status", ML)
    opts = payload["OptionSet"]["Options"]
    assert len(opts) == 3
    # Build value -> labels dict for explicit assertions
    value_to_labels = {o["Value"]: _labels_for(o) for o in opts}
    expected = {
        1: {1033: "Active", 1036: "Actif"},
        2: {1033: "Inactive", 1036: "Inactif"},
        5: {1033: "Archived", 1036: "Archivé"},
    }
    # Assert exact label match per language (no unexpected languages or mismatches)
    for val, exp_map in expected.items():
        assert val in value_to_labels, f"Missing option value {val} in payload"
        got_map = value_to_labels[val]
        assert got_map == exp_map, f"Labels mismatch for value {val}: expected {exp_map}, got {got_map}"

def test_missing_translation_falls_back_to_member_name():
    class PartiallyTranslated(IntEnum):
        Alpha = 1
        Beta = 2
        __labels__ = {1036: {"Alpha": "Alphé"}}  # Only French for Alpha

    c = _make_client(lang=1033)  # Config language 1033 must appear
    payload = c._enum_optionset_payload("new_Code", PartiallyTranslated)
    opts = payload["OptionSet"]["Options"]
    # Build value -> {lang:label}
    m = {o["Value"]: _labels_for(o) for o in opts}
    # Alpha has French custom label, Beta falls back to name for French
    alpha_labels = m[1]
    beta_labels = m[2]
    assert alpha_labels[1036] == "Alphé"
    assert beta_labels[1036] == "Beta"  # fallback
    # Config language 1033 present for both using member names
    assert alpha_labels[1033] == "Alpha"
    assert beta_labels[1033] == "Beta"

def test_labels_accept_member_objects_and_names():
    class Mixed(IntEnum):
        A = 1
        B = 2
        __labels__ = {
            1033: {A: "LetterA", "B": "LetterB"},  # mix member object & name
        }

    c = _make_client()
    payload = c._enum_optionset_payload("new_Mixed", Mixed)
    opts = payload["OptionSet"]["Options"]
    labels_map = {o["Value"]: _labels_for(o) for o in opts}
    assert labels_map[1][1033] == "LetterA"
    assert labels_map[2][1033] == "LetterB"

def test_is_primary_name_flag_propagates():
    class PN(IntEnum):
        X = 1
    c = _make_client()
    payload = c._enum_optionset_payload("new_Status", PN, is_primary_name=True)
    assert payload["IsPrimaryName"] is True

def test_duplicate_enum_values_raise():
    class Dup(IntEnum):
        A = 1
        B = 1
    c = _make_client()
    with pytest.raises(ValueError):
        c._enum_optionset_payload("new_Status", Dup)

def test_non_int_enum_values_raise():
    class Bad(Enum):
        A = "x"
    c = _make_client()
    with pytest.raises(ValueError):
        c._enum_optionset_payload("new_Status", Bad)

def test_enum_labels_not_dict_raises():
    class BadLabels(IntEnum):
        A = 1
        __labels__ = ["not", "a", "dict"]
    c = _make_client()
    with pytest.raises(ValueError):
        c._enum_optionset_payload("new_BadLabels", BadLabels)


def test_enum_labels_language_code_not_int_raises():
    class BadLangKey(IntEnum):
        A = 1
        __labels__ = {
            "en": {"A": "Alpha"},
        }
    c = _make_client()
    with pytest.raises(ValueError):
        c._enum_optionset_payload("new_BadLangKey", BadLangKey)


def test_enum_labels_mapping_not_dict_raises():
    class BadMapping(IntEnum):
        A = 1
        __labels__ = {
            1033: ["A", "Alpha"],
        }
    c = _make_client()
    with pytest.raises(ValueError):
        c._enum_optionset_payload("new_BadMapping", BadMapping)


def test_enum_labels_empty_label_value_raises():
    class EmptyLabel(IntEnum):
        A = 1
        __labels__ = {
            1033: {"A": "  "},
        }
    c = _make_client()
    with pytest.raises(ValueError):
        c._enum_optionset_payload("new_EmptyLabel", EmptyLabel)