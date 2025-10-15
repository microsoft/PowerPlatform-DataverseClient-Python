import pytest
import importlib.resources as ir
from dataverse_sdk.odata import ODataClient


class DummyAuth:
    class _Tok:
        access_token = "dummy"
    def acquire_token(self, scope):  # pragma: no cover - trivial
        return self._Tok()


def _make_client(overrides=None):
    return ODataClient(DummyAuth(), "https://example.crm.dynamics.com", feature_flags=overrides)


# ---------------- JSON schema tests (monkeypatched resource) ----------------

def _patch_json(monkeypatch, tmp_path, content: str):
    p = tmp_path / "feature_flags.json"
    p.write_text(content, encoding="utf-8")

    class _FilesProxy:
        def __init__(self, path):
            self._path = path
        def joinpath(self, name):
            assert name == "feature_flags.json"
            return self._path
    def _fake_files(pkg):
        assert pkg == "dataverse_sdk"
        return _FilesProxy(p)
    monkeypatch.setattr(ir, "files", _fake_files)
    return p


def test_json_bool_shorthand_rejected(monkeypatch, tmp_path):
    _patch_json(monkeypatch, tmp_path, '{"flag1": true}')
    with pytest.raises(RuntimeError, match="Feature 'flag1' must be an object"):
        _make_client()


def test_json_missing_description(monkeypatch, tmp_path):
    _patch_json(monkeypatch, tmp_path, '{"flag1": {"default": true}}')
    with pytest.raises(RuntimeError, match="missing required key\(s\): description"):
        _make_client()


def test_json_unknown_key(monkeypatch, tmp_path):
    _patch_json(monkeypatch, tmp_path, '{"flag1": {"default": true, "description": "ok", "extra": 1}}')
    with pytest.raises(RuntimeError, match="unknown metadata keys: extra"):
        _make_client()


def test_json_blank_description(monkeypatch, tmp_path):
    _patch_json(monkeypatch, tmp_path, '{"flag1": {"default": true, "description": "   "}}')
    with pytest.raises(RuntimeError, match="description' must be a non-empty string"):
        _make_client()


def test_json_default_not_bool(monkeypatch, tmp_path):
    _patch_json(monkeypatch, tmp_path, '{"flag1": {"default": "yes", "description": "x"}}')
    with pytest.raises(RuntimeError, match="'default' must be boolean"):
        _make_client()


def test_valid_json_and_override(monkeypatch, tmp_path):
    _patch_json(monkeypatch, tmp_path, '{"flag1": {"default": false, "description": "desc"}}')
    c = _make_client({"flag1": True})
    assert c.is_feature_enabled("flag1") is True


# ---------------- Override validation tests (using real packaged JSON) ---------------

def test_override_unknown_feature():
    with pytest.raises(ValueError, match="Unknown feature flag override 'does_not_exist'"):
        _make_client({"does_not_exist": True})


def test_override_non_bool():
    with pytest.raises(ValueError, match="must be boolean"):
        _make_client({"option_set_label_conversion": 1})


def test_override_success_real_flag():
    c = _make_client({"option_set_label_conversion": True})
    assert c.is_feature_enabled("option_set_label_conversion") is True


def test_override_empty_key():
    with pytest.raises(ValueError, match="override keys must be non-empty"):
        _make_client({"": True})
