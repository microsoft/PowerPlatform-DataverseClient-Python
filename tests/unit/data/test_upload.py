# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.data._odata import _ODataClient


def _make_odata_client() -> _ODataClient:
    """Return an _ODataClient with HTTP calls mocked out."""
    mock_auth = MagicMock()
    mock_auth._acquire_token.return_value = MagicMock(access_token="token")
    client = _ODataClient(mock_auth, "https://example.crm.dynamics.com")
    client._request = MagicMock()
    return client


def _make_temp_file(content: bytes = b"test content", suffix: str = ".bin") -> str:
    """Create a temporary file and return its path. Caller must delete."""
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(content)
        return f.name


class TestUploadFile(unittest.TestCase):
    """Tests for _upload_file() mode selection, column auto-creation, and argument forwarding."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-1", "LogicalName": "account"}
        )
        self.od._get_attribute_metadata = MagicMock(return_value={"LogicalName": "new_document"})

    def test_auto_mode_small_file(self):
        """Auto mode routes files <128MB to _upload_file_small."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small = MagicMock()
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="auto")
        self.od._upload_file_small.assert_called_once()

    def test_auto_mode_large_file_routes_to_chunk(self):
        """Auto mode routes files >=128MB to _upload_file_chunk."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk = MagicMock()
        with patch("os.path.getsize", return_value=128 * 1024 * 1024):
            self.od._upload_file("account", "guid-1", "new_Document", path, mode="auto")
        self.od._upload_file_chunk.assert_called_once()

    def test_default_mode_is_auto(self):
        """mode=None is treated as 'auto'."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small = MagicMock()
        self.od._upload_file("account", "guid-1", "new_Document", path)
        self.od._upload_file_small.assert_called_once()

    def test_auto_mode_file_not_found(self):
        """Auto mode raises FileNotFoundError for missing file."""
        with self.assertRaises(FileNotFoundError):
            self.od._upload_file("account", "guid-1", "new_Document", "/nonexistent/file.pdf")

    def test_explicit_small_mode(self):
        """Explicit 'small' mode calls _upload_file_small."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small = MagicMock()
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small")
        self.od._upload_file_small.assert_called_once()

    def test_explicit_chunk_mode(self):
        """Explicit 'chunk' mode calls _upload_file_chunk."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk = MagicMock()
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="chunk")
        self.od._upload_file_chunk.assert_called_once()

    def test_invalid_mode_raises(self):
        """Invalid mode raises ValueError."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        with self.assertRaises(ValueError) as ctx:
            self.od._upload_file("account", "guid-1", "new_Document", path, mode="invalid")
        self.assertIn("invalid", str(ctx.exception).lower())

    def test_column_auto_creation_when_missing(self):
        """Creates file column when attribute metadata not found."""
        self.od._get_attribute_metadata = MagicMock(return_value=None)
        self.od._create_columns = MagicMock()
        self.od._wait_for_attribute_visibility = MagicMock()
        self.od._upload_file_small = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small")
        self.od._create_columns.assert_called_once_with("account", {"new_Document": "file"})
        self.od._wait_for_attribute_visibility.assert_called_once_with("accounts", "new_Document")

    def test_column_exists_skips_creation(self):
        """Does not create column when attribute already exists."""
        self.od._create_columns = MagicMock()
        self.od._upload_file_small = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small")
        self.od._create_columns.assert_not_called()

    def test_no_entity_metadata_skips_column_check(self):
        """Skips column check entirely when entity metadata is None."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value=None)
        self.od._get_attribute_metadata = MagicMock()
        self.od._upload_file_small = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small")
        self.od._get_attribute_metadata.assert_not_called()

    def test_entity_metadata_without_metadata_id_skips_column_check(self):
        """Skips attribute check when entity metadata has no MetadataId."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value={"LogicalName": "account"})
        self.od._get_attribute_metadata = MagicMock()
        self.od._upload_file_small = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small")
        self.od._get_attribute_metadata.assert_not_called()

    def test_lowercases_attribute_name(self):
        """File name attribute is lowercased for URL usage."""
        self.od._upload_file_small = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small")
        # Third positional arg to _upload_file_small is the logical_name (lowercased)
        self.assertEqual(self.od._upload_file_small.call_args.args[2], "new_document")

    def test_passes_mime_type_to_small(self):
        """mime_type is forwarded as content_type to _upload_file_small."""
        self.od._upload_file_small = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small", mime_type="text/csv")
        self.assertEqual(self.od._upload_file_small.call_args.kwargs["content_type"], "text/csv")

    def test_passes_if_none_match_to_small(self):
        """if_none_match is forwarded to _upload_file_small."""
        self.od._upload_file_small = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="small", if_none_match=False)
        self.assertFalse(self.od._upload_file_small.call_args.kwargs["if_none_match"])

    def test_passes_if_none_match_to_chunk(self):
        """if_none_match is forwarded to _upload_file_chunk."""
        self.od._upload_file_chunk = MagicMock()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file("account", "guid-1", "new_Document", path, mode="chunk", if_none_match=False)
        self.assertFalse(self.od._upload_file_chunk.call_args.kwargs["if_none_match"])


class TestUploadFileSmall(unittest.TestCase):
    """Tests for _upload_file_small() single PATCH upload."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_successful_upload(self):
        """Sends PATCH with correct URL, headers and file data."""
        path = _make_temp_file(b"PDF file content here", suffix=".pdf")
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small("accounts", "guid-1", "new_document", path)
        self.od._request.assert_called_once()
        call = self.od._request.call_args
        self.assertEqual(call.args[0], "patch")
        self.assertIn("new_document", call.args[1])
        self.assertEqual(call.kwargs["data"], b"PDF file content here")

    def test_url_contains_entity_set_and_record_id(self):
        """URL is constructed from entity_set, record_id, and attribute."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small("accounts", "guid-1", "new_document", path)
        url = self.od._request.call_args.args[1]
        self.assertIn("accounts", url)
        self.assertIn("guid-1", url)
        self.assertIn("new_document", url)

    def test_if_none_match_header(self):
        """if_none_match=True sends If-None-Match: null."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small("accounts", "guid-1", "col", path, if_none_match=True)
        headers = self.od._request.call_args.kwargs["headers"]
        self.assertEqual(headers["If-None-Match"], "null")
        self.assertNotIn("If-Match", headers)

    def test_if_match_overwrite_header(self):
        """if_none_match=False sends If-Match: *."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small("accounts", "guid-1", "col", path, if_none_match=False)
        headers = self.od._request.call_args.kwargs["headers"]
        self.assertEqual(headers["If-Match"], "*")
        self.assertNotIn("If-None-Match", headers)

    def test_custom_mime_type(self):
        """Custom content_type is used in Content-Type header."""
        path = _make_temp_file(b"{}", suffix=".json")
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small("accounts", "guid-1", "col", path, content_type="application/json")
        headers = self.od._request.call_args.kwargs["headers"]
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_default_mime_type(self):
        """Default Content-Type is application/octet-stream."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small("accounts", "guid-1", "col", path)
        headers = self.od._request.call_args.kwargs["headers"]
        self.assertEqual(headers["Content-Type"], "application/octet-stream")

    def test_file_not_found_raises(self):
        """Raises FileNotFoundError for missing file."""
        with self.assertRaises(FileNotFoundError):
            self.od._upload_file_small("accounts", "guid-1", "col", "/no/such/file.txt")

    def test_empty_record_id_raises(self):
        """Raises ValueError for empty record_id."""
        with self.assertRaises(ValueError):
            self.od._upload_file_small("accounts", "", "col", "/any/path")

    def test_file_name_in_header(self):
        """x-ms-file-name header contains the basename of the file."""
        path = _make_temp_file(b"a,b,c", suffix=".csv")
        self.addCleanup(os.unlink, path)
        self.od._upload_file_small("accounts", "guid-1", "col", path)
        headers = self.od._request.call_args.kwargs["headers"]
        self.assertEqual(headers["x-ms-file-name"], os.path.basename(path))

    def test_file_exceeds_small_upload_limit_raises(self):
        """Raises ValueError when file exceeds 128MB single-upload limit."""
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        with patch("os.path.getsize", return_value=128 * 1024 * 1024 + 1):
            with self.assertRaises(ValueError) as ctx:
                self.od._upload_file_small("accounts", "guid-1", "col", path)
        self.assertIn("chunk", str(ctx.exception).lower())


class TestUploadFileChunk(unittest.TestCase):
    """Tests for _upload_file_chunk() streaming chunked upload."""

    def setUp(self):
        self.od = _make_odata_client()

    @staticmethod
    def _mock_init_response(location="https://example.com/session?token=abc", chunk_size=None):
        """Create a mock init PATCH response with Location and optional chunk-size headers."""
        resp = MagicMock()
        headers = {"Location": location}
        if chunk_size is not None:
            headers["x-ms-chunk-size"] = str(chunk_size)
        resp.headers = headers
        return resp

    def test_init_patch_sends_chunked_header(self):
        """Initial PATCH sends x-ms-transfer-mode: chunked."""
        self.od._request.return_value = self._mock_init_response()
        path = _make_temp_file(b"x" * 100)
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        init_call = self.od._request.call_args_list[0]
        self.assertEqual(init_call.kwargs["headers"]["x-ms-transfer-mode"], "chunked")

    def test_init_url_contains_file_name(self):
        """Init PATCH URL includes x-ms-file-name query parameter."""
        self.od._request.return_value = self._mock_init_response()
        path = _make_temp_file(b"data", suffix=".pdf")
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        init_url = self.od._request.call_args_list[0].args[1]
        self.assertIn("x-ms-file-name=", init_url)

    def test_missing_location_header_raises(self):
        """Raises RuntimeError when init response lacks Location header."""
        resp = MagicMock()
        resp.headers = {}
        self.od._request.return_value = resp
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        with self.assertRaises(RuntimeError) as ctx:
            self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        self.assertIn("Location", str(ctx.exception))

    def test_lowercase_location_header_accepted(self):
        """Accepts lowercase 'location' header as fallback."""
        resp = MagicMock()
        resp.headers = {"location": "https://example.com/session?token=abc"}
        self.od._request.return_value = resp
        path = _make_temp_file(b"data")
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        # 1 init + 1 chunk = 2 total calls
        self.assertEqual(self.od._request.call_count, 2)

    def test_uses_chunk_size_from_response(self):
        """Uses x-ms-chunk-size from init response to determine chunk size."""
        self.od._request.return_value = self._mock_init_response(chunk_size=50)
        path = _make_temp_file(b"x" * 120)  # 120 bytes / 50-byte chunks = 3 chunks
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        # 1 init + 3 chunk calls = 4 total
        self.assertEqual(self.od._request.call_count, 4)

    def test_default_chunk_size_when_header_missing(self):
        """Falls back to 4MB chunk size when x-ms-chunk-size header missing."""
        self.od._request.return_value = self._mock_init_response()  # no chunk_size
        path = _make_temp_file(b"x" * 100)  # 100 bytes < 4MB = single chunk
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        # 1 init + 1 chunk = 2 total
        self.assertEqual(self.od._request.call_count, 2)

    def test_malformed_chunk_size_header_falls_back_to_default(self):
        """Non-integer x-ms-chunk-size falls back to 4MB default."""
        resp = MagicMock()
        resp.headers = {"Location": "https://example.com/session", "x-ms-chunk-size": "not-a-number"}
        self.od._request.return_value = resp
        path = _make_temp_file(b"x" * 100)  # 100 bytes < 4MB = single chunk
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        # Falls back to 4MB default → 100 bytes = 1 chunk → 2 total calls
        self.assertEqual(self.od._request.call_count, 2)

    def test_negative_chunk_size_raises(self):
        """Negative x-ms-chunk-size raises ValueError (zero falls back to 4MB default)."""
        resp = MagicMock()
        resp.headers = {"Location": "https://example.com/session", "x-ms-chunk-size": "-1"}
        self.od._request.return_value = resp
        path = _make_temp_file(b"data")
        self.addCleanup(os.unlink, path)
        with self.assertRaises(ValueError):
            self.od._upload_file_chunk("accounts", "guid-1", "col", path)

    def test_empty_file_completes_without_chunk_requests(self):
        """Zero-byte file sends only the init PATCH, no chunk PATCHes."""
        self.od._request.return_value = self._mock_init_response()
        path = _make_temp_file(b"")
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        # Only the init PATCH is sent
        self.assertEqual(self.od._request.call_count, 1)

    def test_content_range_headers(self):
        """Each chunk has correct Content-Range header."""
        self.od._request.return_value = self._mock_init_response(chunk_size=10)
        path = _make_temp_file(b"A" * 10 + b"B" * 10 + b"C" * 5)  # 25 bytes -> 3 chunks
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        chunk_calls = self.od._request.call_args_list[1:]  # skip init
        self.assertEqual(len(chunk_calls), 3)
        self.assertEqual(chunk_calls[0].kwargs["headers"]["Content-Range"], "bytes 0-9/25")
        self.assertEqual(chunk_calls[1].kwargs["headers"]["Content-Range"], "bytes 10-19/25")
        self.assertEqual(chunk_calls[2].kwargs["headers"]["Content-Range"], "bytes 20-24/25")

    def test_chunk_content_length_header(self):
        """Each chunk includes correct Content-Length header."""
        self.od._request.return_value = self._mock_init_response(chunk_size=10)
        path = _make_temp_file(b"A" * 10 + b"B" * 5)  # 15 bytes -> 2 chunks (10 + 5)
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        chunk_calls = self.od._request.call_args_list[1:]
        self.assertEqual(chunk_calls[0].kwargs["headers"]["Content-Length"], "10")
        self.assertEqual(chunk_calls[1].kwargs["headers"]["Content-Length"], "5")

    def test_chunk_sends_to_location_url(self):
        """Chunk PATCHes go to the Location URL, not the original URL."""
        session_url = "https://example.com/upload?session=xyz"
        self.od._request.return_value = self._mock_init_response(location=session_url)
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        chunk_call = self.od._request.call_args_list[1]
        self.assertEqual(chunk_call.args[1], session_url)

    def test_if_none_match_on_init(self):
        """if_none_match=True sends If-None-Match on init PATCH."""
        self.od._request.return_value = self._mock_init_response()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path, if_none_match=True)
        init_headers = self.od._request.call_args_list[0].kwargs["headers"]
        self.assertEqual(init_headers["If-None-Match"], "null")
        self.assertNotIn("If-Match", init_headers)

    def test_if_match_overwrite_on_init(self):
        """if_none_match=False sends If-Match on init PATCH."""
        self.od._request.return_value = self._mock_init_response()
        path = _make_temp_file()
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path, if_none_match=False)
        init_headers = self.od._request.call_args_list[0].kwargs["headers"]
        self.assertEqual(init_headers["If-Match"], "*")
        self.assertNotIn("If-None-Match", init_headers)

    def test_empty_record_id_raises(self):
        """Raises ValueError for empty record_id."""
        with self.assertRaises(ValueError):
            self.od._upload_file_chunk("accounts", "", "col", "/any/path")

    def test_file_not_found_raises(self):
        """Raises FileNotFoundError for missing file."""
        with self.assertRaises(FileNotFoundError):
            self.od._upload_file_chunk("accounts", "guid-1", "col", "/no/such/file.bin")

    def test_chunk_requests_accept_206_and_204(self):
        """Chunk requests use expected=(206, 204)."""
        self.od._request.return_value = self._mock_init_response(chunk_size=50)
        path = _make_temp_file(b"x" * 100)
        self.addCleanup(os.unlink, path)
        self.od._upload_file_chunk("accounts", "guid-1", "col", path)
        for chunk_call in self.od._request.call_args_list[1:]:
            self.assertEqual(chunk_call.kwargs["expected"], (206, 204))
