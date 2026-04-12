# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _HttpLogger and LogConfig."""

from __future__ import annotations

import dataclasses
import os
import re

import pytest

from PowerPlatform.Dataverse.core._http_logger import _HttpLogger
from PowerPlatform.Dataverse.core.config import DataverseConfig
from PowerPlatform.Dataverse.core.log_config import LogConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_logger(tmp_path, **kwargs) -> _HttpLogger:
    """Create an _HttpLogger writing to a temp directory."""
    cfg = LogConfig(log_folder=str(tmp_path), **kwargs)
    return _HttpLogger(cfg)


def _read_log(tmp_path) -> str:
    """Return the concatenated content of all .log files in tmp_path."""
    parts = []
    for fname in sorted(os.listdir(tmp_path)):
        if fname.endswith(".log"):
            with open(os.path.join(tmp_path, fname), encoding="utf-8") as fh:
                parts.append(fh.read())
    return "".join(parts)


# ---------------------------------------------------------------------------
# LogConfig defaults
# ---------------------------------------------------------------------------


def test_log_config_defaults():
    cfg = LogConfig()
    assert cfg.log_folder == "./dataverse_logs"
    assert cfg.log_file_prefix == "dataverse"
    assert cfg.max_body_bytes == 0
    assert cfg.log_level == "DEBUG"
    assert cfg.max_file_bytes == 10_485_760
    assert cfg.backup_count == 5
    assert "authorization" in cfg.redacted_headers
    assert "proxy-authorization" in cfg.redacted_headers


def test_log_config_frozen():
    cfg = LogConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.log_folder = "/other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Log file creation
# ---------------------------------------------------------------------------


def test_log_file_created(tmp_path):
    _make_logger(tmp_path)
    log_files = [f for f in os.listdir(tmp_path) if f.endswith(".log")]
    assert len(log_files) == 1
    # File should match: <prefix>_YYYYMMDD_HHMMSS_microseconds.log
    assert re.match(r"dataverse_\d{8}_\d{6}_\d+\.log", log_files[0])


def test_log_file_custom_prefix(tmp_path):
    _make_logger(tmp_path, log_file_prefix="crm_debug")
    log_files = [f for f in os.listdir(tmp_path) if f.endswith(".log")]
    assert log_files[0].startswith("crm_debug_")


def test_log_folder_created_if_missing(tmp_path):
    nested = os.path.join(str(tmp_path), "deep", "nested")
    cfg = LogConfig(log_folder=nested)
    _HttpLogger(cfg)
    assert os.path.isdir(nested)


# ---------------------------------------------------------------------------
# Header redaction
# ---------------------------------------------------------------------------


def test_authorization_header_redacted(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_request("GET", "https://example.com", headers={"Authorization": "Bearer secret123"})
    content = _read_log(tmp_path)
    assert "secret123" not in content
    assert "[REDACTED]" in content


def test_proxy_authorization_redacted(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_request("GET", "https://example.com", headers={"Proxy-Authorization": "Basic xyz"})
    content = _read_log(tmp_path)
    assert "xyz" not in content
    assert "[REDACTED]" in content


def test_safe_headers_not_redacted(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_request("GET", "https://example.com", headers={"Content-Type": "application/json"})
    content = _read_log(tmp_path)
    assert "application/json" in content


def test_headers_formatted_one_per_line(tmp_path):
    """Each header must appear on its own line as 'key: value', not as a dict repr."""
    logger = _make_logger(tmp_path)
    logger.log_request(
        "GET",
        "https://example.com",
        headers={"Accept": "application/json", "OData-Version": "4.0"},
    )
    content = _read_log(tmp_path)
    assert "    Accept: application/json" in content
    assert "    OData-Version: 4.0" in content
    # Old dict format must not be present
    assert "Headers: {" not in content


def test_case_insensitive_redaction(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_request("GET", "https://example.com", headers={"AUTHORIZATION": "Bearer token"})
    content = _read_log(tmp_path)
    assert "Bearer token" not in content
    assert "[REDACTED]" in content


def test_custom_redacted_headers(tmp_path):
    cfg = LogConfig(log_folder=str(tmp_path), redacted_headers=frozenset({"x-custom-secret"}))
    logger = _HttpLogger(cfg)
    logger.log_request("GET", "https://example.com", headers={"X-Custom-Secret": "my-secret"})
    content = _read_log(tmp_path)
    assert "my-secret" not in content
    assert "[REDACTED]" in content


# ---------------------------------------------------------------------------
# Body truncation
# ---------------------------------------------------------------------------


def test_body_truncation(tmp_path):
    logger = _make_logger(tmp_path, max_body_bytes=10)
    logger.log_request("POST", "https://example.com", body="A" * 100)
    content = _read_log(tmp_path)
    assert "truncated" in content
    assert "100 bytes total" in content


def test_body_not_truncated_when_under_limit(tmp_path):
    logger = _make_logger(tmp_path, max_body_bytes=200)
    logger.log_request("POST", "https://example.com", body="hello world")
    content = _read_log(tmp_path)
    assert "hello world" in content
    assert "truncated" not in content


def test_zero_max_body_bytes_disables_body(tmp_path):
    logger = _make_logger(tmp_path, max_body_bytes=0)
    logger.log_request("POST", "https://example.com", body="should not appear")
    content = _read_log(tmp_path)
    assert "should not appear" not in content


def test_bytes_body_decoded(tmp_path):
    logger = _make_logger(tmp_path, max_body_bytes=1024)
    logger.log_request("POST", "https://example.com", body=b"binary content")
    content = _read_log(tmp_path)
    assert "binary content" in content


def test_dict_body_serialized(tmp_path):
    logger = _make_logger(tmp_path, max_body_bytes=1024)
    logger.log_request("POST", "https://example.com", body={"name": "Contoso"})
    content = _read_log(tmp_path)
    assert "Contoso" in content


# ---------------------------------------------------------------------------
# log_request
# ---------------------------------------------------------------------------


def test_log_request_contains_method_and_url(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_request("POST", "https://example.crm.dynamics.com/api/data/v9.2/accounts")
    content = _read_log(tmp_path)
    assert ">>> REQUEST" in content
    assert "POST" in content
    assert "https://example.crm.dynamics.com/api/data/v9.2/accounts" in content


def test_log_request_no_body_no_body_line(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_request("GET", "https://example.com", body=None)
    content = _read_log(tmp_path)
    assert "Body:" not in content


# ---------------------------------------------------------------------------
# log_response
# ---------------------------------------------------------------------------


def test_log_response_contains_status_and_elapsed(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_response("GET", "https://example.com", status_code=200, elapsed_ms=123.4)
    content = _read_log(tmp_path)
    assert "<<< RESPONSE" in content
    assert "200" in content
    assert "123.4ms" in content


def test_log_response_no_elapsed_when_none(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_response("GET", "https://example.com", status_code=404, elapsed_ms=None)
    content = _read_log(tmp_path)
    assert "<<< RESPONSE" in content
    assert "ms)" not in content


# ---------------------------------------------------------------------------
# log_error
# ---------------------------------------------------------------------------


def test_log_error_writes_error_entry(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log_error("DELETE", "https://example.com/resource", ValueError("connection refused"))
    content = _read_log(tmp_path)
    assert "!!! ERROR" in content
    assert "DELETE" in content
    assert "ValueError" in content
    assert "connection refused" in content


# ---------------------------------------------------------------------------
# Integration: _HttpClient with logger=None (no errors)
# ---------------------------------------------------------------------------


def test_http_client_no_logger_no_errors():
    from unittest.mock import MagicMock

    from PowerPlatform.Dataverse.core._http import _HttpClient

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {}
    mock_resp.text = ""

    session = MagicMock()
    session.request.return_value = mock_resp

    client = _HttpClient(session=session, logger=None)
    resp = client._request("GET", "https://example.com")
    assert resp.status_code == 200


def test_http_client_with_logger_logs_request_and_response(tmp_path):
    from unittest.mock import MagicMock

    from PowerPlatform.Dataverse.core._http import _HttpClient

    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.text = '{"value": "ok"}'

    session = MagicMock()
    session.request.return_value = mock_resp

    cfg = LogConfig(log_folder=str(tmp_path))
    http_logger = _HttpLogger(cfg)
    client = _HttpClient(session=session, logger=http_logger)
    client._request("POST", "https://example.com/api/data/v9.2/accounts", json={"name": "Test"})

    content = _read_log(tmp_path)
    assert ">>> REQUEST" in content
    assert "POST" in content
    assert "<<< RESPONSE" in content
    assert "201" in content


# ---------------------------------------------------------------------------
# Integration: DataverseConfig with log_config
# ---------------------------------------------------------------------------


def test_dataverse_config_log_config_field(tmp_path):
    cfg = LogConfig(log_folder=str(tmp_path))
    dc = DataverseConfig(log_config=cfg)
    assert dc.log_config is cfg


def test_dataverse_config_log_config_default_is_none():
    dc = DataverseConfig()
    assert dc.log_config is None


def test_dataverse_config_from_env_log_config_none():
    dc = DataverseConfig.from_env()
    assert dc.log_config is None


# ---------------------------------------------------------------------------
# Fix #2: empty dict body must be logged, not silently dropped
# ---------------------------------------------------------------------------


def test_http_client_logs_empty_dict_body(tmp_path):
    """An empty JSON body {} is falsy but must still be logged (not skipped via `or`)."""
    from unittest.mock import MagicMock

    from PowerPlatform.Dataverse.core._http import _HttpClient

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {}
    mock_resp.text = ""

    session = MagicMock()
    session.request.return_value = mock_resp

    cfg = LogConfig(log_folder=str(tmp_path), max_body_bytes=4096)
    http_logger = _HttpLogger(cfg)
    client = _HttpClient(session=session, logger=http_logger)
    client._request("POST", "https://example.com/accounts", json={})

    content = _read_log(tmp_path)
    assert ">>> REQUEST" in content
    assert "{}" in content


# ---------------------------------------------------------------------------
# Fix #3: resp.text not decoded when body logging disabled
# ---------------------------------------------------------------------------


def test_http_client_does_not_decode_response_body_when_logging_disabled(tmp_path):
    """When max_body_bytes=0, resp.text must not be accessed (no unnecessary decoding)."""
    from unittest.mock import MagicMock, PropertyMock

    from PowerPlatform.Dataverse.core._http import _HttpClient

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {}
    # If resp.text is accessed, the test will fail
    type(mock_resp).text = PropertyMock(side_effect=AssertionError("resp.text should not be accessed"))

    session = MagicMock()
    session.request.return_value = mock_resp

    cfg = LogConfig(log_folder=str(tmp_path), max_body_bytes=0)
    http_logger = _HttpLogger(cfg)
    client = _HttpClient(session=session, logger=http_logger)
    # Should not raise
    client._request("GET", "https://example.com")


# ---------------------------------------------------------------------------
# Fix #4: _HttpLogger.close() releases file handle
# ---------------------------------------------------------------------------


def test_http_logger_close_releases_handler(tmp_path):
    """close() flushes and removes the handler so the file handle is released."""
    logger = _make_logger(tmp_path)
    logger.log_request("GET", "https://example.com")
    logger.close()
    # After close the internal logger should have no handlers
    assert len(logger._logger.handlers) == 0


def test_http_logger_close_is_idempotent(tmp_path):
    """Calling close() twice must not raise."""
    logger = _make_logger(tmp_path)
    logger.close()
    logger.close()  # should not raise


# ---------------------------------------------------------------------------
# Fix #5: filename uses microsecond precision (no collision)
# ---------------------------------------------------------------------------


def test_log_filenames_unique_for_rapid_creation(tmp_path):
    """Two loggers created back-to-back get distinct filenames."""
    l1 = _make_logger(tmp_path)
    l2 = _make_logger(tmp_path)
    log_files = [f for f in os.listdir(tmp_path) if f.endswith(".log")]
    l1.close()
    l2.close()
    assert len(log_files) == 2
    assert log_files[0] != log_files[1]


# ---------------------------------------------------------------------------
# Fix #6: byte-correct truncation for Unicode bodies
# ---------------------------------------------------------------------------


def test_body_truncation_unicode_byte_accurate(tmp_path):
    """Truncation respects byte budget even for multi-byte Unicode characters."""
    # Each '€' is 3 UTF-8 bytes; 10 bytes limit should cut within a few chars
    logger = _make_logger(tmp_path, max_body_bytes=10)
    body = "€" * 20  # 60 bytes total
    logger.log_request("POST", "https://example.com", body=body)
    content = _read_log(tmp_path)
    assert "truncated" in content
    assert "60 bytes total" in content


def test_body_truncation_reports_byte_count_not_char_count(tmp_path):
    """The truncation message reports UTF-8 byte length, not character count."""
    # 5 chars × 3 bytes each = 15 bytes; limit 5 bytes → should report 15 bytes
    logger = _make_logger(tmp_path, max_body_bytes=5)
    body = "€€€€€"  # 5 chars, 15 bytes
    logger.log_request("POST", "https://example.com", body=body)
    content = _read_log(tmp_path)
    assert "15 bytes total" in content
