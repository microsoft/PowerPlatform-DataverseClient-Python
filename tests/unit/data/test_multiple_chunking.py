# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Comprehensive tests for _create_multiple / _update_multiple / _upsert_multiple
client-side chunking (issue #156).

Coverage goals
--------------
- Boundary conditions: 0, 1, BATCH-1, BATCH, BATCH+1, 2*BATCH, 2*BATCH+1 records
- Chunk sizes: first chunk always full, last chunk carries the remainder
- Payload correctness: each chunk sent to the right endpoint with the right records
- ID aggregation: IDs from all chunks are collected in order
- _update_by_ids: delegates correctly to _update_multiple (broadcast + paired)
- Public API (records.create / records.update / records.upsert): delegates correctly
"""

import unittest
from unittest.mock import MagicMock

from PowerPlatform.Dataverse.data._odata import _MULTIPLE_BATCH_SIZE, _ODataClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_odata_client() -> _ODataClient:
    """Return an _ODataClient with all HTTP calls mocked."""
    mock_auth = MagicMock()
    mock_auth._acquire_token.return_value = MagicMock(access_token="token")
    client = _ODataClient(mock_auth, "https://org.crm.dynamics.com")
    client._request = MagicMock()
    # Skip picklist HTTP calls so _request counts reflect only batch POSTs
    client._convert_labels_to_ints = MagicMock(side_effect=lambda _t, r: r)
    return client


def _mock_create_response(ids):
    """Mock HTTP response returning {"Ids": ids}."""
    resp = MagicMock()
    resp.text = "x"
    resp.json.return_value = {"Ids": ids}
    return resp


def _mock_update_response():
    """Mock HTTP response for UpdateMultiple (no meaningful body)."""
    resp = MagicMock()
    resp.text = ""
    return resp




# ---------------------------------------------------------------------------
# _create_multiple
# ---------------------------------------------------------------------------


class TestCreateMultipleBoundaries(unittest.TestCase):
    """Chunk-count and request-count are correct at every boundary value."""

    def setUp(self):
        self.od = _make_odata_client()

    def _run(self, n, side_effects=None):
        """Call _create_multiple with n records; side_effects controls responses."""
        records = [{"name": f"R{i}"} for i in range(n)]
        if side_effects is None:
            side_effects = [_mock_create_response([f"id-{i}" for i in range(n)])]
        self.od._execute_raw = MagicMock(side_effect=side_effects)
        return self.od._create_multiple("accounts", "account", records)

    def test_zero_records_no_request(self):
        """Empty list produces zero chunks so no request is sent."""
        self.od._execute_raw = MagicMock(return_value=_mock_create_response([]))
        result = self.od._create_multiple("accounts", "account", [])
        self.od._execute_raw.assert_not_called()
        self.assertEqual(result, [])

    def test_one_record_single_request(self):
        """Single record → one request, one ID returned."""
        result = self._run(1, [_mock_create_response(["id-0"])])
        self.od._execute_raw.assert_called_once()
        self.assertEqual(result, ["id-0"])

    def test_batch_minus_one_single_request(self):
        """B-1 records fit in one chunk."""
        ids = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE - 1)]
        result = self._run(_MULTIPLE_BATCH_SIZE - 1, [_mock_create_response(ids)])
        self.od._execute_raw.assert_called_once()
        self.assertEqual(len(result), _MULTIPLE_BATCH_SIZE - 1)

    def test_exact_batch_size_single_request(self):
        """Exactly _MULTIPLE_BATCH_SIZE records → one chunk, one request."""
        ids = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE)]
        result = self._run(_MULTIPLE_BATCH_SIZE, [_mock_create_response(ids)])
        self.od._execute_raw.assert_called_once()
        self.assertEqual(len(result), _MULTIPLE_BATCH_SIZE)

    def test_batch_plus_one_two_requests(self):
        """B+1 records → two chunks, two requests."""
        ids1 = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE)]
        ids2 = ["id-last"]
        result = self._run(_MULTIPLE_BATCH_SIZE + 1, [_mock_create_response(ids1), _mock_create_response(ids2)])
        self.assertEqual(self.od._execute_raw.call_count, 2)
        self.assertEqual(len(result), _MULTIPLE_BATCH_SIZE + 1)

    def test_two_full_batches(self):
        """2*_MULTIPLE_BATCH_SIZE records → two full chunks."""
        ids1 = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE)]
        ids2 = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE, 2 * _MULTIPLE_BATCH_SIZE)]
        result = self._run(2 * _MULTIPLE_BATCH_SIZE, [_mock_create_response(ids1), _mock_create_response(ids2)])
        self.assertEqual(self.od._execute_raw.call_count, 2)
        self.assertEqual(len(result), 2 * _MULTIPLE_BATCH_SIZE)

    def test_two_batches_plus_one(self):
        """2*_MULTIPLE_BATCH_SIZE+1 records → three chunks."""
        se = [_mock_create_response([f"id-{j}" for j in range(_MULTIPLE_BATCH_SIZE)]) for _ in range(2)]
        se.append(_mock_create_response(["id-extra"]))
        result = self._run(2 * _MULTIPLE_BATCH_SIZE + 1, se)
        self.assertEqual(self.od._execute_raw.call_count, 3)
        self.assertEqual(len(result), 2 * _MULTIPLE_BATCH_SIZE + 1)


class TestCreateMultipleChunkPayloads(unittest.TestCase):
    """Each chunk contains exactly the right slice of the input records."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._execute_raw = MagicMock(return_value=_mock_create_response([]))

    def _captured_targets(self, call_index):
        """Return the Targets list from the _build_create_multiple payload for a given call."""
        # _execute_raw is called with the result of _build_create_multiple, which
        # we can't easily inspect without going deeper. Instead, patch _build_create_multiple.
        return None  # handled in test below

    def test_first_chunk_has_batch_size_records(self):
        """The first chunk sent to the server has exactly _MULTIPLE_BATCH_SIZE records."""
        records = [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 50)]
        self.od._execute_raw = MagicMock(return_value=_mock_create_response([]))

        captured = []

        original_build = self.od._build_create_multiple

        def capturing_build(entity_set, table, chunk):
            captured.append(len(chunk))
            return original_build(entity_set, table, chunk)

        self.od._build_create_multiple = capturing_build
        self.od._create_multiple("accounts", "account", records)

        self.assertEqual(captured[0], _MULTIPLE_BATCH_SIZE)
        self.assertEqual(captured[1], 50)

    def test_chunks_cover_all_records_no_overlap(self):
        """All input records appear in exactly one chunk, no duplicates or gaps."""
        n = _MULTIPLE_BATCH_SIZE + 7
        records = [{"name": f"R{i}", "idx": i} for i in range(n)]
        self.od._execute_raw = MagicMock(return_value=_mock_create_response([]))

        seen_indices = []

        original_build = self.od._build_create_multiple

        def capturing_build(entity_set, table, chunk):
            seen_indices.extend(r["idx"] for r in chunk)
            return original_build(entity_set, table, chunk)

        self.od._build_create_multiple = capturing_build
        self.od._create_multiple("accounts", "account", records)

        self.assertEqual(sorted(seen_indices), list(range(n)))


class TestCreateMultipleIdAggregation(unittest.TestCase):
    """IDs from all chunks are aggregated into a single flat list."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_sequential_ids_preserved_in_order(self):
        """Sequential dispatch preserves chunk order in the returned IDs."""
        ids1 = ["a1", "a2"]
        ids2 = ["b1"]
        self.od._execute_raw = MagicMock(
            side_effect=[_mock_create_response(ids1), _mock_create_response(ids2)]
        )
        result = self.od._create_multiple(
            "accounts", "account", [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 1)]
        )
        self.assertEqual(result[:2], ["a1", "a2"])
        self.assertIn("b1", result)
        self.assertEqual(len(result), 3)

    def test_empty_ids_from_chunk_still_aggregated(self):
        """A chunk returning [] does not cause errors; its contribution is simply empty."""
        self.od._execute_raw = MagicMock(
            side_effect=[_mock_create_response([]), _mock_create_response(["id-1"])]
        )
        result = self.od._create_multiple(
            "accounts", "account", [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 1)]
        )
        self.assertEqual(result, ["id-1"])

    def test_value_fallback_response_still_aggregated(self):
        """Chunks whose response uses the 'value' key instead of 'Ids' are handled."""
        resp1 = MagicMock()
        resp1.text = "x"
        resp1.json.return_value = {"Ids": ["a1"]}

        resp2 = MagicMock()
        resp2.text = "x"
        # Simulate alternative response shape (no 'Ids', has 'value')
        resp2.json.return_value = {"value": []}

        self.od._execute_raw = MagicMock(side_effect=[resp1, resp2])
        result = self.od._create_multiple(
            "accounts", "account", [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 1)]
        )
        self.assertIn("a1", result)


class TestCreateMultipleTypeValidation(unittest.TestCase):
    """Input validation is unchanged by chunking."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._execute_raw = MagicMock(return_value=_mock_create_response([]))

    def test_non_dict_in_list_raises_type_error(self):
        """A non-dict element raises TypeError before any HTTP call."""
        with self.assertRaises(TypeError):
            self.od._create_multiple("accounts", "account", [{"name": "ok"}, "bad"])
        self.od._execute_raw.assert_not_called()


# ---------------------------------------------------------------------------
# _update_multiple
# ---------------------------------------------------------------------------


class TestUpdateMultipleBoundaries(unittest.TestCase):
    """Chunk-count is correct at boundary values."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._execute_raw = MagicMock(return_value=_mock_update_response())

    def _records(self, n):
        return [{"accountid": f"id-{i}", "name": f"N{i}"} for i in range(n)]

    def test_one_record_single_request(self):
        self.od._update_multiple("accounts", "account", self._records(1))
        self.od._execute_raw.assert_called_once()

    def test_batch_minus_one_single_request(self):
        self.od._update_multiple("accounts", "account", self._records(_MULTIPLE_BATCH_SIZE - 1))
        self.od._execute_raw.assert_called_once()

    def test_exact_batch_single_request(self):
        self.od._update_multiple("accounts", "account", self._records(_MULTIPLE_BATCH_SIZE))
        self.od._execute_raw.assert_called_once()

    def test_batch_plus_one_two_requests(self):
        self.od._update_multiple("accounts", "account", self._records(_MULTIPLE_BATCH_SIZE + 1))
        self.assertEqual(self.od._execute_raw.call_count, 2)

    def test_two_full_batches(self):
        self.od._update_multiple("accounts", "account", self._records(2 * _MULTIPLE_BATCH_SIZE))
        self.assertEqual(self.od._execute_raw.call_count, 2)

    def test_two_batches_plus_one(self):
        self.od._update_multiple("accounts", "account", self._records(2 * _MULTIPLE_BATCH_SIZE + 1))
        self.assertEqual(self.od._execute_raw.call_count, 3)


class TestUpdateMultipleChunkPayloads(unittest.TestCase):
    """Each chunk contains the right slice of records in the right order."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._execute_raw = MagicMock(return_value=_mock_update_response())

    def test_first_chunk_size_is_batch_size(self):
        """First chunk has exactly _MULTIPLE_BATCH_SIZE records."""
        n = _MULTIPLE_BATCH_SIZE + 17
        records = [{"accountid": f"id-{i}", "name": f"N{i}"} for i in range(n)]
        captured = []
        original = self.od._build_update_multiple_from_records

        def capturing(entity_set, table, chunk):
            captured.append(len(chunk))
            return original(entity_set, table, chunk)

        self.od._build_update_multiple_from_records = capturing
        self.od._update_multiple("accounts", "account", records)
        self.assertEqual(captured[0], _MULTIPLE_BATCH_SIZE)
        self.assertEqual(captured[1], 17)

    def test_records_are_not_duplicated_or_dropped(self):
        """All input IDs appear exactly once across all chunks."""
        n = _MULTIPLE_BATCH_SIZE + 3
        records = [{"accountid": f"id-{i}", "name": f"N{i}"} for i in range(n)]
        seen_ids = []
        original = self.od._build_update_multiple_from_records

        def capturing(entity_set, table, chunk):
            seen_ids.extend(r["accountid"] for r in chunk)
            return original(entity_set, table, chunk)

        self.od._build_update_multiple_from_records = capturing
        self.od._update_multiple("accounts", "account", records)
        self.assertEqual(sorted(seen_ids), sorted(f"id-{i}" for i in range(n)))


class TestUpdateMultipleTypeValidation(unittest.TestCase):
    """Input validation is unchanged by chunking."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._execute_raw = MagicMock(return_value=_mock_update_response())

    def test_empty_list_raises_type_error(self):
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", [])
        self.od._execute_raw.assert_not_called()

    def test_non_list_raises_type_error(self):
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", {"accountid": "x"})  # type: ignore
        self.od._execute_raw.assert_not_called()

    def test_non_dict_element_raises_type_error(self):
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", [{"accountid": "x"}, "bad"])
        self.od._execute_raw.assert_not_called()


# ---------------------------------------------------------------------------
# _upsert_multiple
# ---------------------------------------------------------------------------


def _alt_keys(n):
    return [{"accountnumber": f"ACC-{i}"} for i in range(n)]


def _upsert_records(n):
    return [{"name": f"N{i}"} for i in range(n)]


class TestUpsertMultipleBoundaries(unittest.TestCase):
    """Chunk-count is correct at boundary values."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._request.return_value = MagicMock()

    def test_one_record_single_request(self):
        self.od._upsert_multiple("accounts", "account", _alt_keys(1), _upsert_records(1))
        self.od._request.assert_called_once()

    def test_batch_minus_one_single_request(self):
        n = _MULTIPLE_BATCH_SIZE - 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.od._request.assert_called_once()

    def test_exact_batch_single_request(self):
        self.od._upsert_multiple("accounts", "account", _alt_keys(_MULTIPLE_BATCH_SIZE), _upsert_records(_MULTIPLE_BATCH_SIZE))
        self.od._request.assert_called_once()

    def test_batch_plus_one_two_requests(self):
        n = _MULTIPLE_BATCH_SIZE + 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.assertEqual(self.od._request.call_count, 2)

    def test_two_full_batches(self):
        n = 2 * _MULTIPLE_BATCH_SIZE
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.assertEqual(self.od._request.call_count, 2)

    def test_two_batches_plus_one(self):
        n = 2 * _MULTIPLE_BATCH_SIZE + 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.assertEqual(self.od._request.call_count, 3)


class TestUpsertMultipleChunkPayloads(unittest.TestCase):
    """Each chunk sent to the server contains the right targets."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._request.return_value = MagicMock()

    def test_first_chunk_has_batch_size_targets(self):
        """First POST body has exactly _MULTIPLE_BATCH_SIZE Targets."""
        n = _MULTIPLE_BATCH_SIZE + 10
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        first_call = self.od._request.call_args_list[0]
        targets = first_call.kwargs["json"]["Targets"]
        self.assertEqual(len(targets), _MULTIPLE_BATCH_SIZE)

    def test_last_chunk_has_remainder_targets(self):
        """Last POST body has the remainder."""
        remainder = 7
        n = _MULTIPLE_BATCH_SIZE + remainder
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        last_call = self.od._request.call_args_list[-1]
        targets = last_call.kwargs["json"]["Targets"]
        self.assertEqual(len(targets), remainder)

    def test_all_targets_sent_no_duplicates(self):
        """All accountnumber values appear exactly once across all chunks."""
        n = _MULTIPLE_BATCH_SIZE + 5
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        all_numbers = []
        for c in self.od._request.call_args_list:
            for t in c.kwargs["json"]["Targets"]:
                # @odata.id contains the accountnumber
                all_numbers.append(t.get("@odata.id", ""))
        self.assertEqual(len(all_numbers), n)
        self.assertEqual(len(set(all_numbers)), n)  # no duplicates

    def test_odata_type_injected_in_each_target(self):
        """@odata.type is injected into every target in every chunk."""
        n = _MULTIPLE_BATCH_SIZE + 2
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        for c in self.od._request.call_args_list:
            for t in c.kwargs["json"]["Targets"]:
                self.assertIn("@odata.type", t)
                self.assertEqual(t["@odata.type"], "Microsoft.Dynamics.CRM.account")

    def test_post_url_uses_upsert_multiple_action(self):
        """Every chunk is POSTed to the UpsertMultiple action URL."""
        n = _MULTIPLE_BATCH_SIZE + 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        for c in self.od._request.call_args_list:
            self.assertIn("UpsertMultiple", c.args[1])
            self.assertEqual(c.args[0], "post")


class TestUpsertMultipleValidationUnchanged(unittest.TestCase):
    """Existing validation (length mismatch, key conflicts) still works with chunking."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._request.return_value = MagicMock()

    def test_length_mismatch_raises_value_error(self):
        with self.assertRaises(ValueError, msg="alternate_keys and records must have the same length"):
            self.od._upsert_multiple("accounts", "account", _alt_keys(3), _upsert_records(2))
        self.od._request.assert_not_called()

    def test_key_conflict_raises_value_error(self):
        with self.assertRaises(ValueError, msg="record payload conflicts with alternate_key"):
            self.od._upsert_multiple(
                "accounts",
                "account",
                [{"accountnumber": "ACC-1"}],
                [{"accountnumber": "ACC-DIFFERENT"}],
            )
        self.od._request.assert_not_called()

    def test_large_batch_with_conflict_in_second_chunk_raises(self):
        """Conflict detection happens before chunking so it catches errors in any position."""
        n = _MULTIPLE_BATCH_SIZE + 1
        alt_keys = _alt_keys(n)
        records = _upsert_records(n)
        # Inject a conflict in the last record (would be in the second chunk)
        records[-1] = {"accountnumber": "WRONG"}
        with self.assertRaises(ValueError):
            self.od._upsert_multiple("accounts", "account", alt_keys, records)
        self.od._request.assert_not_called()


# ---------------------------------------------------------------------------
# _update_by_ids — delegates to _update_multiple
# ---------------------------------------------------------------------------


class TestUpdateByIdsDelegation(unittest.TestCase):
    """_update_by_ids builds the correct batch and delegates to _update_multiple."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._update_multiple = MagicMock()
        self.od._primary_id_attr = MagicMock(return_value="accountid")
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")

    def test_broadcast_delegates_correctly(self):
        """Broadcast mode builds one record per ID and delegates."""
        ids = ["id-1", "id-2"]
        self.od._update_by_ids("account", ids, {"name": "X"})
        self.od._update_multiple.assert_called_once_with(
            "accounts", "account", [{"accountid": "id-1", "name": "X"}, {"accountid": "id-2", "name": "X"}]
        )

    def test_paired_delegates_correctly(self):
        """Paired mode merges id+patch per record and delegates."""
        ids = ["id-1", "id-2"]
        changes = [{"name": "A"}, {"name": "B"}]
        self.od._update_by_ids("account", ids, changes)
        self.od._update_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountid": "id-1", "name": "A"}, {"accountid": "id-2", "name": "B"}],
        )


# ---------------------------------------------------------------------------
# Public API: records.create / records.update / records.upsert
# ---------------------------------------------------------------------------


def _make_records_client():
    """Return a DataverseClient with _odata mocked for public API tests."""
    from contextlib import contextmanager

    from PowerPlatform.Dataverse.operations.records import RecordOperations

    mock_client = MagicMock()
    mock_odata = MagicMock()
    mock_odata._entity_set_from_schema_name.return_value = "accounts"
    mock_odata._create_multiple.return_value = ["guid-1", "guid-2"]
    mock_odata._update_by_ids.return_value = None
    mock_odata._upsert_multiple.return_value = None

    @contextmanager
    def scoped():
        yield mock_odata

    mock_client._scoped_odata = scoped
    ops = RecordOperations(mock_client)
    return ops, mock_odata


class TestPublicCreateDelegation(unittest.TestCase):
    """records.create delegates to _create_multiple for list input."""

    def test_list_delegates_to_create_multiple(self):
        ops, mock_odata = _make_records_client()
        ops.create("account", [{"name": "A"}, {"name": "B"}])
        mock_odata._create_multiple.assert_called_once_with(
            "accounts", "account", [{"name": "A"}, {"name": "B"}]
        )

    def test_single_dict_delegates_to_create(self):
        """Single-record create calls _create, not _create_multiple."""
        ops, mock_odata = _make_records_client()
        mock_odata._create.return_value = "guid-single"
        ops.create("account", {"name": "A"})
        mock_odata._create_multiple.assert_not_called()
        mock_odata._create.assert_called_once()


class TestPublicUpdateDelegation(unittest.TestCase):
    """records.update delegates to _update_by_ids for list input."""

    def test_list_delegates_to_update_by_ids(self):
        ops, mock_odata = _make_records_client()
        ops.update("account", ["id-1", "id-2"], {"name": "X"})
        mock_odata._update_by_ids.assert_called_once_with(
            "account", ["id-1", "id-2"], {"name": "X"}
        )

    def test_single_delegates_to_update(self):
        """Single-record update calls _update, not _update_by_ids."""
        ops, mock_odata = _make_records_client()
        ops.update("account", "id-1", {"name": "X"})
        mock_odata._update_by_ids.assert_not_called()
        mock_odata._update.assert_called_once()


class TestPublicUpsertDelegation(unittest.TestCase):
    """records.upsert delegates to _upsert_multiple for multi-item input."""

    def test_multi_item_delegates_to_upsert_multiple(self):
        from PowerPlatform.Dataverse.models.upsert import UpsertItem

        ops, mock_odata = _make_records_client()
        items = [
            UpsertItem(alternate_key={"accountnumber": "A1"}, record={"name": "Contoso"}),
            UpsertItem(alternate_key={"accountnumber": "A2"}, record={"name": "Fabrikam"}),
        ]
        ops.upsert("account", items)
        mock_odata._upsert_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountnumber": "A1"}, {"accountnumber": "A2"}],
            [{"name": "Contoso"}, {"name": "Fabrikam"}],
        )

    def test_single_item_delegates_to_upsert(self):
        """A single-item list calls _upsert (PATCH), not _upsert_multiple."""
        from PowerPlatform.Dataverse.models.upsert import UpsertItem

        ops, mock_odata = _make_records_client()
        ops.upsert("account", [UpsertItem(alternate_key={"accountnumber": "A1"}, record={"name": "X"})])
        mock_odata._upsert_multiple.assert_not_called()
        mock_odata._upsert.assert_called_once()


if __name__ == "__main__":
    unittest.main()
